require "./utils"

module Crysda
  MISSING_VALUE = "NA"

  record ColumnFormula, name : String, expression : TableExpression
  record AggFunc, value : SumFormula, suffix : String? = nil
  record ColSpec, pos : Int32, name : String, type : String

  alias SumFormula = DataCol -> Any

  class ColumnException < Exception
  end

  class InvalidColumnOperationException < Exception
  end

  class MissingValueException < Exception
  end

  class InvalidSortingPredicateException < Exception
    def initialize(val)
      super("Sorting literal did not evaluate to boolean array, but instead to literal '#{val}'")
    end
  end

  class NonScalarValueException < Exception
    def initialize(tf : ColumnFormula, result)
      super("summarize() expression for '#{tf.name}' did not evaluate into a scalar value but into a #{result}")
    end
  end

  # ===========================================================================
  # Null Bitmap - 1 bit per element for efficient null tracking
  # Uses UInt64 words with SIMD acceleration for bulk operations
  # ===========================================================================
  struct NullBitmap
    @bits : Slice(UInt64)
    @size : Int32
    @has_any : Bool

    # SIMD threshold - use SIMD for bitmaps with >= this many words
    SIMD_THRESHOLD = 4

    def initialize(@size : Int32)
      num_words = (@size + 63) >> 6
      @bits = Slice(UInt64).new(num_words, 0_u64)
      @has_any = false
    end

    def self.none(size : Int32) : NullBitmap
      new(size)
    end

    def self.from_nullable(arr : Array) : NullBitmap
      bm = new(arr.size)
      arr.each_with_index { |v, i| bm.set(i) if v.nil? }
      bm
    end

    # Build bitmap directly from parsing - avoids intermediate Array(T?)
    def self.build(size : Int32, &) : NullBitmap
      bm = new(size)
      size.times { |i| bm.set(i) if yield i }
      bm
    end

    protected def set_has_any(val : Bool)
      @has_any = val
    end

    @[AlwaysInline]
    def [](index : Int32) : Bool
      word = index >> 6
      bit = index & 63
      (@bits.unsafe_fetch(word) & (1_u64 << bit)) != 0
    end

    def set(index : Int32) : Nil
      word = index >> 6
      bit = index & 63
      @bits[word] |= (1_u64 << bit)
      @has_any = true
    end

    @[AlwaysInline]
    def clear(index : Int32) : Nil
      word = index >> 6
      bit = index & 63
      @bits[word] &= ~(1_u64 << bit)
    end

    @[AlwaysInline]
    def any? : Bool
      @has_any
    end

    @[AlwaysInline]
    def none? : Bool
      !@has_any
    end

    # Optimized count using SIMD popcount where available
    def count : Int32
      return 0 unless @has_any
      {% if flag?(:aarch64) %}
        simd_popcount_neon
      {% elsif flag?(:x86_64) %}
        simd_popcount_sse
      {% else %}
        scalar_popcount
      {% end %}
    end

    # Scalar fallback for popcount
    private def scalar_popcount : Int32
      total = 0
      @bits.each { |w| total += w.popcount }
      total.to_i32
    end

    {% if flag?(:x86_64) %}
      # SSE optimized popcount for x86_64
      # Processes 2 x 64-bit words at a time using XMM registers
      private def simd_popcount_sse : Int32
        return scalar_popcount if @bits.size < SIMD_THRESHOLD

        total = 0_i64
        num_pairs = @bits.size >> 1
        remainder = @bits.size & 1

        if num_pairs > 0
          ptr = @bits.to_unsafe
          result = uninitialized Int64
          result_ptr = pointerof(result)

          num_pairs.times do |i|
            # Use popcnt instruction directly on each 64-bit word
            w1 = ptr[i * 2]
            w2 = ptr[i * 2 + 1]
            total += w1.popcount + w2.popcount
          end
        end

        # Handle remainder
        if remainder > 0
          total += @bits[@bits.size - 1].popcount
        end

        total.to_i32
      end
    {% end %}

    {% if flag?(:aarch64) %}
      # NEON optimized popcount for AArch64
      # Uses cnt instruction which counts bits in each byte
      private def simd_popcount_neon : Int32
        return scalar_popcount if @bits.size < SIMD_THRESHOLD

        total = 0_i64
        ptr = @bits.to_unsafe
        num_pairs = @bits.size >> 1
        remainder = @bits.size & 1

        # Process 128 bits (2 x 64-bit words) at a time
        num_pairs.times do |i|
          pair_ptr = ptr + (i * 2)
          pair_count = 0_i64

          asm(
            "ld1 {v0.2d}, [$1]       // load 2 x 64-bit words
             cnt v0.16b, v0.16b      // count bits in each byte
             uaddlv h1, v0.16b       // sum all bytes into h1
             smov $0, v1.h[0]        // move result to output (smov for signed extend)"
                  : "=r"(pair_count)
                  : "r"(pair_ptr)
                  : "v0", "v1"
                  : "volatile"
          )
          total += pair_count
        end

        # Handle remainder
        if remainder > 0
          total += @bits[@bits.size - 1].popcount
        end

        total.to_i32
      end
    {% end %}

    # Optimized OR - uses SIMD for large bitmaps
    def |(other : NullBitmap) : NullBitmap
      result = NullBitmap.new(@size)
      {% if flag?(:aarch64) %}
        simd_or_neon(other, result)
      {% elsif flag?(:x86_64) %}
        simd_or_sse(other, result)
      {% else %}
        scalar_or(other, result)
      {% end %}
      result.set_has_any(@has_any || other.@has_any)
      result
    end

    # Scalar OR fallback
    private def scalar_or(other : NullBitmap, result : NullBitmap) : Nil
      @bits.each_with_index { |w, i| result.@bits[i] = w | other.@bits.unsafe_fetch(i) }
    end

    {% if flag?(:x86_64) %}
      # SSE optimized OR for x86_64
      private def simd_or_sse(other : NullBitmap, result : NullBitmap) : Nil
        if @bits.size < SIMD_THRESHOLD
          scalar_or(other, result)
          return
        end

        a_ptr = @bits.to_unsafe
        b_ptr = other.@bits.to_unsafe
        r_ptr = result.@bits.to_unsafe
        num_pairs = @bits.size >> 1
        remainder = @bits.size & 1

        # Process 128 bits at a time (2 x 64-bit words)
        num_pairs.times do |i|
          offset = i * 2
          asm(
            "movdqu ($1), %xmm0      // load 128 bits from a
           movdqu ($2), %xmm1      // load 128 bits from b
           por %xmm1, %xmm0        // OR operation
           movdqu %xmm0, ($0)      // store result"
                  :: "r"(r_ptr + offset), "r"(a_ptr + offset), "r"(b_ptr + offset)
                  : "xmm0", "xmm1", "memory"
                  : "volatile"
          )
        end

        # Handle remainder
        if remainder > 0
          idx = @bits.size - 1
          result.@bits[idx] = @bits[idx] | other.@bits[idx]
        end
      end
    {% end %}

    {% if flag?(:aarch64) %}
      # NEON optimized OR for AArch64
      private def simd_or_neon(other : NullBitmap, result : NullBitmap) : Nil
        if @bits.size < SIMD_THRESHOLD
          scalar_or(other, result)
          return
        end

        a_ptr = @bits.to_unsafe
        b_ptr = other.@bits.to_unsafe
        r_ptr = result.@bits.to_unsafe
        num_pairs = @bits.size >> 1
        remainder = @bits.size & 1

        # Process 128 bits at a time (2 x 64-bit words)
        num_pairs.times do |i|
          offset = i * 2
          asm(
            "ld1 {v0.2d}, [$1]       // load 128 bits from a
           ld1 {v1.2d}, [$2]       // load 128 bits from b
           orr v2.16b, v0.16b, v1.16b  // OR operation
           st1 {v2.2d}, [$0]       // store result"
                  :: "r"(r_ptr + offset), "r"(a_ptr + offset), "r"(b_ptr + offset)
                  : "v0", "v1", "v2", "memory"
                  : "volatile"
          )
        end

        # Handle remainder
        if remainder > 0
          idx = @bits.size - 1
          result.@bits[idx] = @bits[idx] | other.@bits[idx]
        end
      end
    {% end %}

    # Optimized AND - uses SIMD for large bitmaps
    def &(other : NullBitmap) : NullBitmap
      result = NullBitmap.new(@size)
      has_any = false
      {% if flag?(:aarch64) %}
        has_any = simd_and_neon(other, result)
      {% elsif flag?(:x86_64) %}
        has_any = simd_and_sse(other, result)
      {% else %}
        has_any = scalar_and(other, result)
      {% end %}
      result.set_has_any(has_any)
      result
    end

    # Scalar AND fallback
    private def scalar_and(other : NullBitmap, result : NullBitmap) : Bool
      has_any = false
      @bits.each_with_index do |w, i|
        val = w & other.@bits.unsafe_fetch(i)
        result.@bits[i] = val
        has_any = true if val != 0
      end
      has_any
    end

    {% if flag?(:x86_64) %}
      # SSE optimized AND for x86_64
      private def simd_and_sse(other : NullBitmap, result : NullBitmap) : Bool
        if @bits.size < SIMD_THRESHOLD
          return scalar_and(other, result)
        end

        a_ptr = @bits.to_unsafe
        b_ptr = other.@bits.to_unsafe
        r_ptr = result.@bits.to_unsafe
        num_pairs = @bits.size >> 1
        remainder = @bits.size & 1

        # Process 128 bits at a time
        num_pairs.times do |i|
          offset = i * 2
          asm(
            "movdqu ($1), %xmm0      // load 128 bits from a
           movdqu ($2), %xmm1      // load 128 bits from b
           pand %xmm1, %xmm0       // AND operation
           movdqu %xmm0, ($0)      // store result"
                  :: "r"(r_ptr + offset), "r"(a_ptr + offset), "r"(b_ptr + offset)
                  : "xmm0", "xmm1", "memory"
                  : "volatile"
          )
        end

        # Handle remainder
        if remainder > 0
          idx = @bits.size - 1
          result.@bits[idx] = @bits[idx] & other.@bits[idx]
        end

        # Check if any bits are set
        result.@bits.any? { |w| w != 0 }
      end
    {% end %}

    {% if flag?(:aarch64) %}
      # NEON optimized AND for AArch64
      private def simd_and_neon(other : NullBitmap, result : NullBitmap) : Bool
        if @bits.size < SIMD_THRESHOLD
          return scalar_and(other, result)
        end

        a_ptr = @bits.to_unsafe
        b_ptr = other.@bits.to_unsafe
        r_ptr = result.@bits.to_unsafe
        num_pairs = @bits.size >> 1
        remainder = @bits.size & 1

        # Process 128 bits at a time
        num_pairs.times do |i|
          offset = i * 2
          asm(
            "ld1 {v0.2d}, [$1]       // load 128 bits from a
           ld1 {v1.2d}, [$2]       // load 128 bits from b
           and v2.16b, v0.16b, v1.16b  // AND operation
           st1 {v2.2d}, [$0]       // store result"
                  :: "r"(r_ptr + offset), "r"(a_ptr + offset), "r"(b_ptr + offset)
                  : "v0", "v1", "v2", "memory"
                  : "volatile"
          )
        end

        # Handle remainder
        if remainder > 0
          idx = @bits.size - 1
          result.@bits[idx] = @bits[idx] & other.@bits[idx]
        end

        # Check if any bits are set
        result.@bits.any? { |w| w != 0 }
      end
    {% end %}

    def size : Int32
      @size
    end

    # Direct access to underlying bits for advanced operations
    protected def bits : Slice(UInt64)
      @bits
    end
  end

  module AggFuncs
    extend self

    def mean
      AggFunc.new(SumFormula.new(&.mean), "mean")
    end

    def median
      AggFunc.new(SumFormula.new(&.median), "median")
    end

    def sd
      AggFunc.new(SumFormula.new(&.sd), "sd")
    end

    def n
      AggFunc.new(SumFormula.new(&.size), "n")
    end

    def na
      AggFunc.new(SumFormula.new(&.is_na.filter { |_| true }.size), "na")
    end
  end

  abstract struct DataCol
    getter name : String

    def initialize(@name)
    end

    def +(val : Number)
      plus(val)
    end

    def +(val : DataCol)
      plus(val)
    end

    def +(val : Iterable)
      plus(Utils.handle_union(Crysda.temp_colname, val.to_a))
    end

    def -(val : Number)
      minus(val)
    end

    def -(val : DataCol)
      minus(val)
    end

    def /(val : Number)
      div(val)
    end

    def /(val : DataCol)
      div(val)
    end

    def *(val : Number)
      times(val)
    end

    def *(val : DataCol)
      times(val)
    end

    def +(val : String)
      res = case self
            when StringCol then self.values.map { |v| self.na_aware_plus(v.as?(String), val) }
            else
              self.values.map { |v| (v.nil? ? MISSING_VALUE : v.to_s) + val }
            end
      StringCol.new(Crysda.temp_colname, Array(String?).new(res.size) { |i| res[i] })
    end

    def -
      self * -1
    end

    {% for op in %w(> >= < <=) %}
    def {{op.id}}(val : Any | DataCol)
      raise UnSupportedOperationException.new
    end
    {% end %}

    def ==(i : Any)
      values.map { |v| v == i }
    end

    def plus(val)
      raise UnSupportedOperationException.new
    end

    def minus(val)
      raise UnSupportedOperationException.new
    end

    def div(val)
      raise UnSupportedOperationException.new
    end

    def times(val)
      raise UnSupportedOperationException.new
    end

    abstract def values : Array
    abstract def compare(left : Int32, right : Int32, null_last = true) : Int32

    def order(na_last = true)
      (0..(values.size - 1)).to_a.sort { |a, b| self.compare(a, b, na_last) }
    end

    def rank(na_last = true)
      order(na_last).map_with_index { |v, i| {i, v} }.sort_by!(&.[1]).map(&.[0])
    end

    def desc
      Int32Col.new(Crysda.temp_colname, rank(false).map(&.-))
    end

    def has_nulls?
      values.any?(Nil)
    end

    def map(&)
      values.map do |v|
        if v.nil?
          nil
        else
          if v.is_a?(ArrayList)
            yield v.as(ArrayList).values
          else
            yield v
          end
        end
      end
    end

    def is_na
      values.map(&.nil?)
    end

    def is_not_na
      values.map { |v| !v.nil? }
    end

    def min(remove_na = false)
      case e = self
      when Int32Col   then e.opt_min(remove_na)
      when Int64Col   then e.opt_min(remove_na)
      when Float64Col then e.opt_min(remove_na)
      else
        raise InvalidColumnOperationException.new
      end
    end

    def max(remove_na = false)
      case e = self
      when Int32Col   then e.opt_max(remove_na)
      when Int64Col   then e.opt_max(remove_na)
      when Float64Col then e.opt_max(remove_na)
      else
        raise InvalidColumnOperationException.new
      end
    end

    def mean(remove_na = false)
      case e = self
      when Int32Col   then e.opt_mean(remove_na)
      when Int64Col   then e.opt_mean(remove_na)
      when Float64Col then e.opt_mean(remove_na)
      else
        raise InvalidColumnOperationException.new
      end
    end

    def sum(remove_na = false)
      case e = self
      when Int32Col   then e.opt_sum(remove_na)
      when Int64Col   then e.opt_sum(remove_na)
      when Float64Col then e.opt_sum(remove_na)
      else
        raise InvalidColumnOperationException.new
      end
    end

    def median(remove_na = false)
      case e = self
      when Int32Col   then e.opt_median(remove_na)
      when Int64Col   then e.opt_median(remove_na)
      when Float64Col then e.opt_median(remove_na)
      else
        raise InvalidColumnOperationException.new
      end
    end

    def sd(remove_na = false)
      case e = self
      when Int32Col   then e.opt_sd(remove_na)
      when Int64Col   then e.opt_sd(remove_na)
      when Float64Col then e.opt_sd(remove_na)
      else
        raise InvalidColumnOperationException.new
      end
    end

    def cumsum
      case e = self
      when Float64Col
        e.values.skip(1).reduce([e.values.first]) do |list, val|
          list + [list.last?.try { |v| (v.nil? || val.nil?) ? nil : (v.not_nil! + val.not_nil!) }]
        end
      when Int32Col
        e.values.skip(1).reduce([e.values.first]) do |list, val|
          list + [list.last?.try { |v| (v.nil? || val.nil?) ? nil : (v.not_nil! + val.not_nil!) }]
        end
      when Int64Col
        e.values.skip(1).reduce([e.values.first]) do |list, val|
          list + [list.last?.try { |v| (v.nil? || val.nil?) ? nil : (v.not_nil! + val.not_nil!) }]
        end
      else
        raise InvalidColumnOperationException.new
      end
    end

    def pct_change : DataCol
      self / lag(1) + (-1)
    end

    def lead(n = 1, default : Any = nil)
      val = case col = self
            when StringCol  then col.values.lead(n, default.as?(String?))
            when Float64Col then col.values.lead(n, default.as?(Float64?))
            when Int32Col   then col.values.lead(n, default.as?(Int32?))
            when Int64Col   then col.values.lead(n, default.as?(Int64?))
            when BoolCol    then col.values.lead(n, default.as?(Bool?))
            when AnyCol     then col.values.lead(n, default)
            else
              raise InvalidColumnOperationException.new
            end
      Utils.handle_union(Crysda.temp_colname, val)
    end

    def lag(n = 1, default : Any = nil)
      val = case col = self
            when StringCol  then col.values.lag(n, default.as?(String?))
            when Float64Col then col.values.lag(n, default.as?(Float64?))
            when Int32Col   then col.values.lag(n, default.as?(Int32?))
            when Int64Col   then col.values.lag(n, default.as?(Int64?))
            when BoolCol    then col.values.lag(n, default.as?(Bool?))
            when AnyCol     then col.values.lag(n, default)
            else
              raise InvalidColumnOperationException.new
            end
      Utils.handle_union(Crysda.temp_colname, val)
    end

    def matching(missing_as = false, &) : Array(Bool)
      map { |e| yield e.to_s }.map { |e| e.nil? ? missing_as : e.not_nil! }
    end

    def as_s
      case self
      when Int32Col, Int64Col, Float64Col, BoolCol, AnyCol
        Array(String?).new(values.size) { |i| values[i].to_s }
      else
        Cast(StringCol).cast(self).values
      end
    end

    def as_b
      Cast(BoolCol).cast(self).values
    end

    def as_i
      Cast(Int32Col).cast(self).values
    end

    def as_i64
      case self
      when Int32Col then Array(Int64?).new(values.size) { |i| self[i].try &.to_i64 }
      else
        Cast(Int64Col).cast(self).values
      end
    end

    def as_f64
      case self
      when Int32Col then Array(Float64?).new(values.size) { |i| self[i].try &.to_f64 }
      when Int64Col then Array(Float64?).new(values.size) { |i| self[i].try &.to_f64 }
      else
        Cast(Float64Col).cast(self).values
      end
    end

    def to_s(io : IO) : Nil
      io << to_s
    end

    def to_s
      prefix = "#{@name} [#{Crysda.get_col_type(self)}][#{size}]: "
      peek = values[..255].map { |v| v.nil? ? "NA" : (v.is_a?(DataFrame) ? "<DataFrame [#{v.num_row} x #{v.num_col}]>" : v) }.join(", ")
      disp_size = Crysda::PRINT_MAX_WIDTH - prefix.size
      w = peek[..disp_size]
      count = w.size - 1
      while peek.size > disp_size && count > 0 && !w[count].in? [',', ' ']
        count -= 1
      end
      ret = prefix + w[...count + 1]
      ret += "..." if count < (w.size - 1)
      ret
    end

    def equals(other : self)
      name == other.name && values.size == other.values.size && values == other.values
    end

    def_equals_and_hash @name, :size, :values
    delegate :[], :[]?, :size, to: values

    private module Cast(R)
      def self.cast(col : DataCol)
        v = col.as?(R)
        raise CrysdaException.new("Could not cast column '#{col.name}' of type '#{col.class.name}' to type '#{R}'") if v.nil?
        v
      end
    end
  end

  # ===========================================================================
  # Optimized Float64 Column - uses Slice + NullBitmap internally
  # ===========================================================================
  struct Float64Col < DataCol
    @data : Slice(Float64)
    @null_bitmap : NullBitmap
    @cached_values : Array(Float64?)?

    def initialize(@name : String, val : Array(Float64?))
      super(@name)
      @data = Slice(Float64).new(val.size, 0.0)
      @null_bitmap = NullBitmap.new(val.size)
      val.each_with_index do |v, i|
        if v.nil?
          @null_bitmap.set(i)
        else
          @data[i] = v
        end
      end
      @cached_values = nil
    end

    protected def initialize(@name : String, @data : Slice(Float64), @null_bitmap : NullBitmap)
      super(@name)
      @cached_values = nil
    end

    def self.dense(name : String, arr : Array(Float64)) : Float64Col
      data = Slice(Float64).new(arr.size) { |i| arr.unsafe_fetch(i).as(Float64) }
      new(name, data, NullBitmap.none(arr.size))
    end

    @[AlwaysInline]
    def has_nulls? : Bool
      @null_bitmap.any?
    end

    def values : Array(Float64?)
      @cached_values ||= Array(Float64?).new(@data.size) do |i|
        @null_bitmap[i] ? nil : @data.unsafe_fetch(i)
      end
    end

    # Lazy iteration - yields values without materializing full array
    def each(&) : Nil
      @data.size.times do |i|
        yield @null_bitmap[i] ? nil : @data.unsafe_fetch(i)
      end
    end

    # Lazy iteration with index
    def each_with_index(&) : Nil
      @data.size.times do |i|
        yield (@null_bitmap[i] ? nil : @data.unsafe_fetch(i)), i
      end
    end

    # Lazy iteration over non-null values only (fast path)
    def each_non_null(&) : Nil
      unless has_nulls?
        @data.each { |v| yield v }
        return
      end
      @data.size.times do |i|
        yield @data.unsafe_fetch(i) unless @null_bitmap[i]
      end
    end

    # Direct index access without array materialization
    @[AlwaysInline]
    def unsafe_fetch(index : Int32) : Float64?
      @null_bitmap[index] ? nil : @data.unsafe_fetch(index)
    end

    protected def raw_data : Slice(Float64)
      @data
    end

    protected def bitmap : NullBitmap
      @null_bitmap
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a_null = @null_bitmap[left]
      b_null = @null_bitmap[right]
      a = a_null ? nil : @data.unsafe_fetch(left)
      b = b_null ? nil : @data.unsafe_fetch(right)
      case
      when a == b then 0
      when a.nil? then null_last ? 1 : -1
      when b.nil? then null_last ? -1 : 1
      else
        a.not_nil! <=> b.not_nil! || (null_last ? -1 : 1)
      end
    end

    {% for op in %w(> >= < <= ==) %}
    def {{op.id}}(val : Number)
      unless has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} val }
      end
      Array(Bool).new(@data.size) { |i| !@null_bitmap[i] && @data.unsafe_fetch(i) {{op.id}} val }
    end

    def {{op.id}}(val : Float64Col)
      unless has_nulls? || val.has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} val.raw_data.unsafe_fetch(i) }
      end
      Array(Bool).new(@data.size) do |i|
        !@null_bitmap[i] && !val.bitmap[i] && @data.unsafe_fetch(i) {{op.id}} val.raw_data.unsafe_fetch(i)
      end
    end
    {% end %}

    def plus(val)
      case val
      when Float64Col then add_col(val)
      when Int32Col, Int64Col
        f64_val = Float64Col.new(val.name, val.values.map { |v| v.try &.to_f64 })
        add_col(f64_val)
      when Number then add_scalar(val.to_f64)
      else             raise UnSupportedOperationException.new
      end
    end

    def minus(val)
      case val
      when Float64Col then sub_col(val)
      when Int32Col, Int64Col
        f64_val = Float64Col.new(val.name, val.values.map { |v| v.try &.to_f64 })
        sub_col(f64_val)
      when Number then sub_scalar(val.to_f64)
      else             raise UnSupportedOperationException.new
      end
    end

    def div(val)
      case val
      when Float64Col then div_col(val)
      when Int32Col, Int64Col
        f64_val = Float64Col.new(val.name, val.values.map { |v| v.try &.to_f64 })
        div_col(f64_val)
      when Number then div_scalar(val.to_f64)
      else             raise UnSupportedOperationException.new
      end
    end

    def times(val)
      case val
      when Float64Col then mul_col(val)
      when Int32Col, Int64Col
        f64_val = Float64Col.new(val.name, val.values.map { |v| v.try &.to_f64 })
        mul_col(f64_val)
      when Number then mul_scalar(val.to_f64)
      else             raise UnSupportedOperationException.new
      end
    end

    # Explicit operations to avoid type inference issues with Proc
    private def add_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def sub_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def mul_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def div_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i) / other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i) / other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def add_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i) + scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def sub_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i) - scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def mul_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i) * scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def div_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i) / scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # =========================================================================
    # Optimized Aggregations - operate directly on Slice, no allocation
    # =========================================================================

    def opt_sum(remove_na = false) : Float64
      # FAST PATH: no nulls
      unless has_nulls?
        total = 0.0
        @data.each { |v| total += v }
        return total
      end
      # Has nulls - check policy
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      # SLOW PATH: skip nulls
      total = 0.0
      @data.size.times { |i| total += @data.unsafe_fetch(i) unless @null_bitmap[i] }
      total
    end

    def opt_mean(remove_na = false) : Float64
      unless has_nulls?
        total = 0.0
        @data.each { |v| total += v }
        return total / @data.size
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      total = 0.0
      count = 0
      @data.size.times do |i|
        unless @null_bitmap[i]
          total += @data.unsafe_fetch(i)
          count += 1
        end
      end
      total / count
    end

    def opt_min(remove_na = false) : Float64
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v < result }
        return result
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      result = Float64::MAX
      found = false
      @data.size.times do |i|
        unless @null_bitmap[i]
          v = @data.unsafe_fetch(i)
          if !found || v < result
            result = v
            found = true
          end
        end
      end
      result
    end

    def opt_max(remove_na = false) : Float64
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v > result }
        return result
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      result = -Float64::MAX
      found = false
      @data.size.times do |i|
        unless @null_bitmap[i]
          v = @data.unsafe_fetch(i)
          if !found || v > result
            result = v
            found = true
          end
        end
      end
      result
    end

    def opt_median(remove_na = false) : Float64
      unless has_nulls?
        sorted = @data.to_a.sort!
        mid = sorted.size // 2
        return sorted.size.odd? ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2.0
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      # Collect non-null values
      arr = Array(Float64).new(@data.size - @null_bitmap.count)
      @data.size.times { |i| arr << @data.unsafe_fetch(i) unless @null_bitmap[i] }
      arr.sort!
      mid = arr.size // 2
      arr.size.odd? ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2.0
    end

    def opt_sd(remove_na = false) : Float64
      m = opt_mean(remove_na)
      unless has_nulls?
        sum_sq = 0.0
        @data.each { |v| sum_sq += (v - m) ** 2 }
        return Math.sqrt(sum_sq / (@data.size - 1))
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      sum_sq = 0.0
      count = 0
      @data.size.times do |i|
        unless @null_bitmap[i]
          sum_sq += (@data.unsafe_fetch(i) - m) ** 2
          count += 1
        end
      end
      Math.sqrt(sum_sq / (count - 1))
    end
  end

  # ===========================================================================
  # Optimized Int32 Column
  # ===========================================================================
  struct Int32Col < DataCol
    @data : Slice(Int32)
    @null_bitmap : NullBitmap
    @cached_values : Array(Int32?)?

    def initialize(@name : String, val : Array(Int32?))
      super(@name)
      @data = Slice(Int32).new(val.size, 0)
      @null_bitmap = NullBitmap.new(val.size)
      val.each_with_index do |v, i|
        if v.nil?
          @null_bitmap.set(i)
        else
          @data[i] = v
        end
      end
      @cached_values = nil
    end

    protected def initialize(@name : String, @data : Slice(Int32), @null_bitmap : NullBitmap)
      super(@name)
      @cached_values = nil
    end

    def self.dense(name : String, arr : Array(Int32)) : Int32Col
      data = Slice(Int32).new(arr.size) { |i| arr.unsafe_fetch(i).as(Int32) }
      new(name, data, NullBitmap.none(arr.size))
    end

    @[AlwaysInline]
    def has_nulls? : Bool
      @null_bitmap.any?
    end

    def values : Array(Int32?)
      @cached_values ||= Array(Int32?).new(@data.size) do |i|
        @null_bitmap[i] ? nil : @data.unsafe_fetch(i)
      end
    end

    # Lazy iteration - yields values without materializing full array
    def each(&) : Nil
      @data.size.times do |i|
        yield @null_bitmap[i] ? nil : @data.unsafe_fetch(i)
      end
    end

    # Lazy iteration with index
    def each_with_index(&) : Nil
      @data.size.times do |i|
        yield (@null_bitmap[i] ? nil : @data.unsafe_fetch(i)), i
      end
    end

    # Lazy iteration over non-null values only (fast path)
    def each_non_null(&) : Nil
      unless has_nulls?
        @data.each { |v| yield v }
        return
      end
      @data.size.times do |i|
        yield @data.unsafe_fetch(i) unless @null_bitmap[i]
      end
    end

    # Direct index access without array materialization
    @[AlwaysInline]
    def unsafe_fetch(index : Int32) : Int32?
      @null_bitmap[index] ? nil : @data.unsafe_fetch(index)
    end

    protected def raw_data : Slice(Int32)
      @data
    end

    protected def bitmap : NullBitmap
      @null_bitmap
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a_null = @null_bitmap[left]
      b_null = @null_bitmap[right]
      a = a_null ? nil : @data.unsafe_fetch(left)
      b = b_null ? nil : @data.unsafe_fetch(right)
      case
      when a == b then 0
      when a.nil? then null_last ? 1 : -1
      when b.nil? then null_last ? -1 : 1
      else
        a.not_nil! <=> b.not_nil! || (null_last ? -1 : 1)
      end
    end

    {% for op in %w(> >= < <= ==) %}
    def {{op.id}}(val : Number)
      unless has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} val }
      end
      Array(Bool).new(@data.size) { |i| !@null_bitmap[i] && @data.unsafe_fetch(i) {{op.id}} val }
    end

    def {{op.id}}(val : Int32Col | Int64Col | Float64Col)
      unless has_nulls? || val.has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} val.raw_data.unsafe_fetch(i) }
      end
      Array(Bool).new(@data.size) do |i|
        !@null_bitmap[i] && !val.bitmap[i] && @data.unsafe_fetch(i) {{op.id}} val.raw_data.unsafe_fetch(i)
      end
    end
    {% end %}

    def plus(val)
      case val
      when Int32Col   then add_i32_col(val)
      when Int64Col   then add_i64_col(val)
      when Float64Col then add_f64_col(val)
      when Int32      then add_i32_scalar(val)
      when Float64    then add_f64_scalar(val)
      else                 raise CrysdaException.new("Unsupported + operation for type #{val.class}")
      end
    end

    def minus(val)
      case val
      when Int32Col   then sub_i32_col(val)
      when Int64Col   then sub_i64_col(val)
      when Float64Col then sub_f64_col(val)
      when Int32      then sub_i32_scalar(val)
      when Float64    then sub_f64_scalar(val)
      else                 raise CrysdaException.new("Unsupported - operation for type #{val.class}")
      end
    end

    def div(val)
      case val
      when Int32Col   then div_i32_col(val)
      when Float64Col then div_f64_col(val)
      when Number     then div_f64_scalar(val.to_f64)
      else                 raise CrysdaException.new("Unsupported / operation for type #{val.class}")
      end
    end

    def times(val)
      case val
      when Int32Col   then mul_i32_col(val)
      when Int64Col   then mul_i64_col(val)
      when Float64Col then mul_f64_col(val)
      when Int32      then mul_i32_scalar(val)
      when Float64    then mul_f64_scalar(val)
      else                 raise CrysdaException.new("Unsupported * operation for type #{val.class}")
      end
    end

    # Int32 operations
    private def add_i32_col(other : Int32Col) : Int32Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int32).new(@data.size) { |i| (@data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i)).as(Int32) }
        return Int32Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int32).new(@data.size) { |i| (result_bitmap[i] ? 0 : @data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i)).as(Int32) }
      Int32Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def sub_i32_col(other : Int32Col) : Int32Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int32).new(@data.size) { |i| (@data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i)).as(Int32) }
        return Int32Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int32).new(@data.size) { |i| (result_bitmap[i] ? 0 : @data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i)).as(Int32) }
      Int32Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def mul_i32_col(other : Int32Col) : Int32Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int32).new(@data.size) { |i| (@data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i)).as(Int32) }
        return Int32Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int32).new(@data.size) { |i| (result_bitmap[i] ? 0 : @data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i)).as(Int32) }
      Int32Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def div_i32_col(other : Int32Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 / other.raw_data.unsafe_fetch(i).to_f64).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i).to_f64 / other.raw_data.unsafe_fetch(i).to_f64).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def add_i32_scalar(scalar : Int32) : Int32Col
      result = Slice(Int32).new(@data.size) { |i| (@data.unsafe_fetch(i) + scalar).as(Int32) }
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def sub_i32_scalar(scalar : Int32) : Int32Col
      result = Slice(Int32).new(@data.size) { |i| (@data.unsafe_fetch(i) - scalar).as(Int32) }
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def mul_i32_scalar(scalar : Int32) : Int32Col
      result = Slice(Int32).new(@data.size) { |i| (@data.unsafe_fetch(i) * scalar).as(Int32) }
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Int64 operations
    private def add_i64_col(other : Int64Col) : Int32Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int32).new(@data.size) { |i| (@data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i).to_i32).as(Int32) }
        return Int32Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int32).new(@data.size) { |i| (result_bitmap[i] ? 0 : @data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i).to_i32).as(Int32) }
      Int32Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def sub_i64_col(other : Int64Col) : Int32Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int32).new(@data.size) { |i| (@data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i).to_i32).as(Int32) }
        return Int32Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int32).new(@data.size) { |i| (result_bitmap[i] ? 0 : @data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i).to_i32).as(Int32) }
      Int32Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def mul_i64_col(other : Int64Col) : Int32Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int32).new(@data.size) { |i| (@data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i).to_i32).as(Int32) }
        return Int32Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int32).new(@data.size) { |i| (result_bitmap[i] ? 0 : @data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i).to_i32).as(Int32) }
      Int32Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    # Float64 operations
    private def add_f64_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 + other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i).to_f64 + other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def sub_f64_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 - other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i).to_f64 - other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def mul_f64_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 * other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i).to_f64 * other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def div_f64_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 / other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i).to_f64 / other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def add_f64_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 + scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def sub_f64_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 - scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def mul_f64_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 * scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def div_f64_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 / scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # =========================================================================
    # Optimized Aggregations
    # =========================================================================

    def opt_sum(remove_na = false) : Int64
      unless has_nulls?
        total = 0_i64
        @data.each { |v| total += v }
        return total
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      total = 0_i64
      @data.size.times { |i| total += @data.unsafe_fetch(i) unless @null_bitmap[i] }
      total
    end

    def opt_mean(remove_na = false) : Float64
      unless has_nulls?
        total = 0_i64
        @data.each { |v| total += v }
        return total.to_f64 / @data.size
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      total = 0_i64
      count = 0
      @data.size.times do |i|
        unless @null_bitmap[i]
          total += @data.unsafe_fetch(i)
          count += 1
        end
      end
      total.to_f64 / count
    end

    def opt_min(remove_na = false) : Int32
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v < result }
        return result
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      result = Int32::MAX
      found = false
      @data.size.times do |i|
        unless @null_bitmap[i]
          v = @data.unsafe_fetch(i)
          if !found || v < result
            result = v
            found = true
          end
        end
      end
      result
    end

    def opt_max(remove_na = false) : Int32
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v > result }
        return result
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      result = Int32::MIN
      found = false
      @data.size.times do |i|
        unless @null_bitmap[i]
          v = @data.unsafe_fetch(i)
          if !found || v > result
            result = v
            found = true
          end
        end
      end
      result
    end

    def opt_median(remove_na = false) : Float64
      unless has_nulls?
        sorted = @data.to_a.sort!
        mid = sorted.size // 2
        return sorted.size.odd? ? sorted[mid].to_f64 : (sorted[mid - 1] + sorted[mid]).to_f64 / 2.0
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      arr = Array(Int32).new(@data.size - @null_bitmap.count)
      @data.size.times { |i| arr << @data.unsafe_fetch(i) unless @null_bitmap[i] }
      arr.sort!
      mid = arr.size // 2
      arr.size.odd? ? arr[mid].to_f64 : (arr[mid - 1] + arr[mid]).to_f64 / 2.0
    end

    def opt_sd(remove_na = false) : Float64
      m = opt_mean(remove_na)
      unless has_nulls?
        sum_sq = 0.0
        @data.each { |v| sum_sq += (v - m) ** 2 }
        return Math.sqrt(sum_sq / (@data.size - 1))
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      sum_sq = 0.0
      count = 0
      @data.size.times do |i|
        unless @null_bitmap[i]
          sum_sq += (@data.unsafe_fetch(i) - m) ** 2
          count += 1
        end
      end
      Math.sqrt(sum_sq / (count - 1))
    end
  end

  # ===========================================================================
  # Optimized Int64 Column
  # ===========================================================================
  struct Int64Col < DataCol
    @data : Slice(Int64)
    @null_bitmap : NullBitmap
    @cached_values : Array(Int64?)?

    def initialize(@name : String, val : Array(Int64?))
      super(@name)
      @data = Slice(Int64).new(val.size, 0_i64)
      @null_bitmap = NullBitmap.new(val.size)
      val.each_with_index do |v, i|
        if v.nil?
          @null_bitmap.set(i)
        else
          @data[i] = v
        end
      end
      @cached_values = nil
    end

    protected def initialize(@name : String, @data : Slice(Int64), @null_bitmap : NullBitmap)
      super(@name)
      @cached_values = nil
    end

    def self.dense(name : String, arr : Array(Int64)) : Int64Col
      data = Slice(Int64).new(arr.size) { |i| arr.unsafe_fetch(i).as(Int64) }
      new(name, data, NullBitmap.none(arr.size))
    end

    @[AlwaysInline]
    def has_nulls? : Bool
      @null_bitmap.any?
    end

    def values : Array(Int64?)
      @cached_values ||= Array(Int64?).new(@data.size) do |i|
        @null_bitmap[i] ? nil : @data.unsafe_fetch(i)
      end
    end

    # Lazy iteration - yields values without materializing full array
    def each(&) : Nil
      @data.size.times do |i|
        yield @null_bitmap[i] ? nil : @data.unsafe_fetch(i)
      end
    end

    # Lazy iteration with index
    def each_with_index(&) : Nil
      @data.size.times do |i|
        yield (@null_bitmap[i] ? nil : @data.unsafe_fetch(i)), i
      end
    end

    # Lazy iteration over non-null values only (fast path)
    def each_non_null(&) : Nil
      unless has_nulls?
        @data.each { |v| yield v }
        return
      end
      @data.size.times do |i|
        yield @data.unsafe_fetch(i) unless @null_bitmap[i]
      end
    end

    # Direct index access without array materialization
    @[AlwaysInline]
    def unsafe_fetch(index : Int32) : Int64?
      @null_bitmap[index] ? nil : @data.unsafe_fetch(index)
    end

    protected def raw_data : Slice(Int64)
      @data
    end

    protected def bitmap : NullBitmap
      @null_bitmap
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a_null = @null_bitmap[left]
      b_null = @null_bitmap[right]
      a = a_null ? nil : @data.unsafe_fetch(left)
      b = b_null ? nil : @data.unsafe_fetch(right)
      case
      when a == b then 0
      when a.nil? then null_last ? 1 : -1
      when b.nil? then null_last ? -1 : 1
      else
        a.not_nil! <=> b.not_nil! || (null_last ? -1 : 1)
      end
    end

    {% for op in %w(> >= < <= ==) %}
    def {{op.id}}(val : Number)
      unless has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} val }
      end
      Array(Bool).new(@data.size) { |i| !@null_bitmap[i] && @data.unsafe_fetch(i) {{op.id}} val }
    end

    def {{op.id}}(val : Int64Col)
      unless has_nulls? || val.has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} val.raw_data.unsafe_fetch(i) }
      end
      Array(Bool).new(@data.size) do |i|
        !@null_bitmap[i] && !val.bitmap[i] && @data.unsafe_fetch(i) {{op.id}} val.raw_data.unsafe_fetch(i)
      end
    end
    {% end %}

    def plus(val)
      case val
      when Int64Col   then add_i64_col(val)
      when Int32Col   then add_i32_col(val)
      when Float64Col then add_f64_col(val)
      when Int64      then add_i64_scalar(val)
      when Int32      then add_i64_scalar(val.to_i64)
      when Float64    then add_f64_scalar(val)
      else                 raise CrysdaException.new("Unsupported + operation for type #{val.class}")
      end
    end

    def minus(val)
      case val
      when Int64Col   then sub_i64_col(val)
      when Int32Col   then sub_i32_col(val)
      when Float64Col then sub_f64_col(val)
      when Int64      then sub_i64_scalar(val)
      when Int32      then sub_i64_scalar(val.to_i64)
      when Float64    then sub_f64_scalar(val)
      else                 raise CrysdaException.new("Unsupported - operation for type #{val.class}")
      end
    end

    def div(val)
      case val
      when Float64Col then div_f64_col(val)
      when Number     then div_f64_scalar(val.to_f64)
      else                 raise CrysdaException.new("Unsupported / operation for type #{val.class}")
      end
    end

    def times(val)
      case val
      when Int64Col   then mul_i64_col(val)
      when Int32Col   then mul_i32_col(val)
      when Float64Col then mul_f64_col(val)
      when Int64      then mul_i64_scalar(val)
      when Int32      then mul_i64_scalar(val.to_i64)
      when Float64    then mul_f64_scalar(val)
      else                 raise CrysdaException.new("Unsupported * operation for type #{val.class}")
      end
    end

    # Int64 operations
    private def add_i64_col(other : Int64Col) : Int64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int64).new(@data.size) { |i| (@data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i)).as(Int64) }
        return Int64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int64).new(@data.size) { |i| (result_bitmap[i] ? 0_i64 : @data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i)).as(Int64) }
      Int64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def sub_i64_col(other : Int64Col) : Int64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int64).new(@data.size) { |i| (@data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i)).as(Int64) }
        return Int64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int64).new(@data.size) { |i| (result_bitmap[i] ? 0_i64 : @data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i)).as(Int64) }
      Int64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def mul_i64_col(other : Int64Col) : Int64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int64).new(@data.size) { |i| (@data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i)).as(Int64) }
        return Int64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int64).new(@data.size) { |i| (result_bitmap[i] ? 0_i64 : @data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i)).as(Int64) }
      Int64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def add_i64_scalar(scalar : Int64) : Int64Col
      result = Slice(Int64).new(@data.size) { |i| (@data.unsafe_fetch(i) + scalar).as(Int64) }
      Int64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def sub_i64_scalar(scalar : Int64) : Int64Col
      result = Slice(Int64).new(@data.size) { |i| (@data.unsafe_fetch(i) - scalar).as(Int64) }
      Int64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def mul_i64_scalar(scalar : Int64) : Int64Col
      result = Slice(Int64).new(@data.size) { |i| (@data.unsafe_fetch(i) * scalar).as(Int64) }
      Int64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Int32 operations
    private def add_i32_col(other : Int32Col) : Int64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int64).new(@data.size) { |i| (@data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i).to_i64).as(Int64) }
        return Int64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int64).new(@data.size) { |i| (result_bitmap[i] ? 0_i64 : @data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i).to_i64).as(Int64) }
      Int64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def sub_i32_col(other : Int32Col) : Int64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int64).new(@data.size) { |i| (@data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i).to_i64).as(Int64) }
        return Int64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int64).new(@data.size) { |i| (result_bitmap[i] ? 0_i64 : @data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i).to_i64).as(Int64) }
      Int64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def mul_i32_col(other : Int32Col) : Int64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Int64).new(@data.size) { |i| (@data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i).to_i64).as(Int64) }
        return Int64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Int64).new(@data.size) { |i| (result_bitmap[i] ? 0_i64 : @data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i).to_i64).as(Int64) }
      Int64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    # Float64 operations
    private def add_f64_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 + other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i).to_f64 + other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def sub_f64_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 - other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i).to_f64 - other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def mul_f64_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 * other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i).to_f64 * other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def div_f64_col(other : Float64Col) : Float64Col
      unless has_nulls? || other.has_nulls?
        result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 / other.raw_data.unsafe_fetch(i)).as(Float64) }
        return Float64Col.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(Float64).new(@data.size) { |i| (result_bitmap[i] ? 0.0 : @data.unsafe_fetch(i).to_f64 / other.raw_data.unsafe_fetch(i)).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def add_f64_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 + scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def sub_f64_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 - scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def mul_f64_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 * scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def div_f64_scalar(scalar : Float64) : Float64Col
      result = Slice(Float64).new(@data.size) { |i| (@data.unsafe_fetch(i).to_f64 / scalar).as(Float64) }
      Float64Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # =========================================================================
    # Optimized Aggregations
    # =========================================================================

    def opt_sum(remove_na = false) : Int64
      unless has_nulls?
        total = 0_i64
        @data.each { |v| total += v }
        return total
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      total = 0_i64
      @data.size.times { |i| total += @data.unsafe_fetch(i) unless @null_bitmap[i] }
      total
    end

    def opt_mean(remove_na = false) : Float64
      unless has_nulls?
        total = 0_i64
        @data.each { |v| total += v }
        return total.to_f64 / @data.size
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      total = 0_i64
      count = 0
      @data.size.times do |i|
        unless @null_bitmap[i]
          total += @data.unsafe_fetch(i)
          count += 1
        end
      end
      total.to_f64 / count
    end

    def opt_min(remove_na = false) : Int64
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v < result }
        return result
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      result = Int64::MAX
      found = false
      @data.size.times do |i|
        unless @null_bitmap[i]
          v = @data.unsafe_fetch(i)
          if !found || v < result
            result = v
            found = true
          end
        end
      end
      result
    end

    def opt_max(remove_na = false) : Int64
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v > result }
        return result
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      result = Int64::MIN
      found = false
      @data.size.times do |i|
        unless @null_bitmap[i]
          v = @data.unsafe_fetch(i)
          if !found || v > result
            result = v
            found = true
          end
        end
      end
      result
    end

    def opt_median(remove_na = false) : Float64
      unless has_nulls?
        sorted = @data.to_a.sort!
        mid = sorted.size // 2
        return sorted.size.odd? ? sorted[mid].to_f64 : (sorted[mid - 1] + sorted[mid]).to_f64 / 2.0
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      arr = Array(Int64).new(@data.size - @null_bitmap.count)
      @data.size.times { |i| arr << @data.unsafe_fetch(i) unless @null_bitmap[i] }
      arr.sort!
      mid = arr.size // 2
      arr.size.odd? ? arr[mid].to_f64 : (arr[mid - 1] + arr[mid]).to_f64 / 2.0
    end

    def opt_sd(remove_na = false) : Float64
      m = opt_mean(remove_na)
      unless has_nulls?
        sum_sq = 0.0
        @data.each { |v| sum_sq += (v - m) ** 2 }
        return Math.sqrt(sum_sq / (@data.size - 1))
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      sum_sq = 0.0
      count = 0
      @data.size.times do |i|
        unless @null_bitmap[i]
          sum_sq += (@data.unsafe_fetch(i) - m) ** 2
          count += 1
        end
      end
      Math.sqrt(sum_sq / (count - 1))
    end
  end

  # ===========================================================================
  # String Pool for String Interning (Dictionary Encoding)
  # Reduces memory for categorical columns with repeated values
  # ===========================================================================
  class StringPool
    @pool : Hash(String, String)
    @stats_hits : Int64
    @stats_misses : Int64

    def initialize(initial_capacity : Int32 = 64)
      @pool = Hash(String, String).new(initial_capacity: initial_capacity)
      @stats_hits = 0_i64
      @stats_misses = 0_i64
    end

    # Intern a string - returns the canonical instance
    @[AlwaysInline]
    def intern(str : String) : String
      if existing = @pool[str]?
        @stats_hits += 1
        existing
      else
        @stats_misses += 1
        @pool[str] = str
        str
      end
    end

    # Intern a nullable string
    @[AlwaysInline]
    def intern(str : String?) : String?
      str.nil? ? nil : intern(str)
    end

    # Number of unique strings in pool
    def size : Int32
      @pool.size
    end

    # Check if interning would be beneficial (high repetition ratio)
    def beneficial?(total_strings : Int32) : Bool
      return false if total_strings == 0
      # Beneficial if we have < 50% unique strings
      @pool.size < (total_strings / 2)
    end

    # Statistics
    def hit_ratio : Float64
      total = @stats_hits + @stats_misses
      return 0.0 if total == 0
      @stats_hits.to_f64 / total
    end

    def clear
      @pool.clear
      @stats_hits = 0_i64
      @stats_misses = 0_i64
    end
  end

  # Global string pool for categorical columns (thread-local would be better for MT)
  @@global_string_pool : StringPool = StringPool.new

  def self.string_pool : StringPool
    @@global_string_pool
  end

  # ===========================================================================
  # String Column
  # ===========================================================================
  struct StringCol < DataCol
    @data : Array(String)
    @null_bitmap : NullBitmap
    @cached_values : Array(String?)?

    def initialize(@name : String, val : Array(String?))
      super(@name)
      @data = Array(String).new(val.size, "")
      @null_bitmap = NullBitmap.new(val.size)
      val.each_with_index do |v, i|
        if v.nil?
          @null_bitmap.set(i)
        else
          @data[i] = v
        end
      end
      @cached_values = nil
    end

    protected def initialize(@name : String, @data : Array(String), @null_bitmap : NullBitmap)
      super(@name)
      @cached_values = nil
    end

    # Create StringCol with string interning for categorical data
    def self.interned(name : String, val : Array(String?)) : StringCol
      pool = StringPool.new
      data = Array(String).new(val.size, "")
      bitmap = NullBitmap.new(val.size)

      val.each_with_index do |v, i|
        if v.nil?
          bitmap.set(i)
        else
          data[i] = pool.intern(v)
        end
      end

      new(name, data, bitmap)
    end

    # Create from raw data with interning
    def self.interned(name : String, data : Array(String), bitmap : NullBitmap) : StringCol
      pool = StringPool.new
      interned_data = Array(String).new(data.size) { |i| pool.intern(data.unsafe_fetch(i)) }
      new(name, interned_data, bitmap)
    end

    @[AlwaysInline]
    def has_nulls? : Bool
      @null_bitmap.any?
    end

    def values : Array(String?)
      @cached_values ||= Array(String?).new(@data.size) do |i|
        @null_bitmap[i] ? nil : @data.unsafe_fetch(i)
      end
    end

    # Lazy iteration - yields values without materializing full array
    def each(&) : Nil
      @data.size.times do |i|
        yield @null_bitmap[i] ? nil : @data.unsafe_fetch(i)
      end
    end

    # Lazy iteration with index
    def each_with_index(&) : Nil
      @data.size.times do |i|
        yield (@null_bitmap[i] ? nil : @data.unsafe_fetch(i)), i
      end
    end

    # Lazy iteration over non-null values only (fast path)
    def each_non_null(&) : Nil
      unless has_nulls?
        @data.each { |v| yield v }
        return
      end
      @data.size.times do |i|
        yield @data.unsafe_fetch(i) unless @null_bitmap[i]
      end
    end

    # Direct index access without array materialization
    @[AlwaysInline]
    def unsafe_fetch(index : Int32) : String?
      @null_bitmap[index] ? nil : @data.unsafe_fetch(index)
    end

    protected def raw_data : Array(String)
      @data
    end

    protected def bitmap : NullBitmap
      @null_bitmap
    end

    # Get unique values count (useful for determining if interning is beneficial)
    def unique_count : Int32
      @data.to_set.size
    end

    # Check if this column is categorical (low cardinality)
    def categorical?(threshold : Float64 = 0.5) : Bool
      return false if @data.empty?
      unique_count.to_f64 / @data.size < threshold
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a_null = @null_bitmap[left]
      b_null = @null_bitmap[right]
      a = a_null ? nil : @data.unsafe_fetch(left)
      b = b_null ? nil : @data.unsafe_fetch(right)
      case
      when a == b then 0
      when a.nil? then null_last ? 1 : -1
      when b.nil? then null_last ? -1 : 1
      else
        a.not_nil! <=> b.not_nil! || (null_last ? -1 : 1)
      end
    end

    {% for op in %w(> >= < <=) %}
    def {{op.id}}(val : String)
      unless has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} val }
      end
      Array(Bool).new(@data.size) { |i| !@null_bitmap[i] && @data.unsafe_fetch(i) {{op.id}} val }
    end

    def {{op.id}}(val : StringCol)
      unless has_nulls? || val.has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} val.raw_data.unsafe_fetch(i) }
      end
      Array(Bool).new(@data.size) do |i|
        !@null_bitmap[i] && !val.bitmap[i] && @data.unsafe_fetch(i) {{op.id}} val.raw_data.unsafe_fetch(i)
      end
    end
    {% end %}

    def plus(val : DataCol)
      raise UnSupportedOperationException.new unless val.is_a?(DataCol)
      StringCol.new(Crysda.temp_colname,
        values.map_with_index { |v, i| na_aware_plus(v, val.values[i].to_s) })
    end

    protected def na_aware_plus(first : String?, second : String?)
      (first.nil? || second.nil?) ? nil : first.not_nil! + second.not_nil!
    end
  end

  # ===========================================================================
  # Bool Column
  # ===========================================================================
  struct BoolCol < DataCol
    def initialize(@name : String, val : Array(Bool?))
      super(@name)
      @values = Array(Bool?).new(val.size) { |i| val[i] }
    end

    def values : Array(Bool?)
      @values
    end

    def has_nulls? : Bool
      @values.any?(Nil)
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a = @values[left]
      b = @values[right]
      case
      when a == b then 0
      when a.nil? then null_last ? 1 : -1
      when b.nil? then null_last ? -1 : 1
      else
        (a != b) ? a ? -1 : 1 : 0
      end
    end
  end

  # ===========================================================================
  # Any Column
  # ===========================================================================
  struct AnyCol < DataCol
    def initialize(@name : String, val : Array(Any))
      super(@name)
      @values = Array(Any?).new(val.size) { |i| val[i] }
    end

    def values : Array(Any)
      @values
    end

    def has_nulls? : Bool
      @values.any?(Nil)
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a = @values[left]
      b = @values[right]
      case
      when a == b then 0
      when a.nil? then null_last ? 1 : -1
      when b.nil? then null_last ? -1 : 1
      else
        (a != b) ? a ? -1 : 1 : 0
      end
    end
  end

  # ===========================================================================
  # DataFrame Column
  # ===========================================================================
  struct DFCol < DataCol
    def initialize(@name : String, val : Array(DataFrame?))
      super(@name)
      @values = Array(DataFrame?).new(val.size) { |i| val[i] }
    end

    def values : Array(DataFrame?)
      @values
    end

    def has_nulls? : Bool
      @values.any?(Nil)
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a = @values[left]
      b = @values[right]
      case
      when a == b then 0
      when a.nil? then null_last ? 1 : -1
      when b.nil? then null_last ? -1 : 1
      else
        (a != b) ? a ? -1 : 1 : 0
      end
    end
  end
end
