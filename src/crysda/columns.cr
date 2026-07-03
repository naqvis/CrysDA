require "big"
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
      when Int32Col      then e.opt_min(remove_na)
      when Int64Col      then e.opt_min(remove_na)
      when Float64Col    then e.opt_min(remove_na)
      when BigDecimalCol then e.opt_min(remove_na)
      else
        raise InvalidColumnOperationException.new
      end
    end

    def max(remove_na = false)
      case e = self
      when Int32Col      then e.opt_max(remove_na)
      when Int64Col      then e.opt_max(remove_na)
      when Float64Col    then e.opt_max(remove_na)
      when BigDecimalCol then e.opt_max(remove_na)
      else
        raise InvalidColumnOperationException.new
      end
    end

    def mean(remove_na = false)
      case e = self
      when Int32Col      then e.opt_mean(remove_na)
      when Int64Col      then e.opt_mean(remove_na)
      when Float64Col    then e.opt_mean(remove_na)
      when BigDecimalCol then e.opt_mean(remove_na)
      else
        raise InvalidColumnOperationException.new
      end
    end

    def sum(remove_na = false)
      case e = self
      when Int32Col      then e.opt_sum(remove_na)
      when Int64Col      then e.opt_sum(remove_na)
      when Float64Col    then e.opt_sum(remove_na)
      when BigDecimalCol then e.opt_sum(remove_na)
      else
        raise InvalidColumnOperationException.new
      end
    end

    def median(remove_na = false)
      case e = self
      when Int32Col      then e.opt_median(remove_na)
      when Int64Col      then e.opt_median(remove_na)
      when Float64Col    then e.opt_median(remove_na)
      when BigDecimalCol then e.opt_median(remove_na)
      else
        raise InvalidColumnOperationException.new
      end
    end

    def sd(remove_na = false)
      case e = self
      when Int32Col      then e.opt_sd(remove_na)
      when Int64Col      then e.opt_sd(remove_na)
      when Float64Col    then e.opt_sd(remove_na)
      when BigDecimalCol then e.opt_sd(remove_na)
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

    # Get first non-null value from column
    def first : Any
      values.each { |v| return v.as(Any) unless v.nil? }
      nil
    end

    # Get last non-null value from column
    def last : Any
      values.reverse_each { |v| return v.as(Any) unless v.nil? }
      nil
    end

    def pct_change : DataCol
      self / lag(1) + (-1)
    end

    def lead(n = 1, default : Any = nil)
      val = case col = self
            when StringCol     then col.values.lead(n, default.as?(String?))
            when Float64Col    then col.values.lead(n, default.as?(Float64?))
            when Int32Col      then col.values.lead(n, default.as?(Int32?))
            when Int64Col      then col.values.lead(n, default.as?(Int64?))
            when BoolCol       then col.values.lead(n, default.as?(Bool?))
            when DateTimeCol   then col.values.lead(n, default.as?(Time?))
            when TimestampCol  then col.values.lead(n, default.as?(Time?))
            when BigDecimalCol then col.values.lead(n, default.as?(BigDecimal?))
            when AnyCol        then col.values.lead(n, default)
            else
              raise InvalidColumnOperationException.new
            end
      Utils.handle_union(Crysda.temp_colname, val)
    end

    def lag(n = 1, default : Any = nil)
      val = case col = self
            when StringCol     then col.values.lag(n, default.as?(String?))
            when Float64Col    then col.values.lag(n, default.as?(Float64?))
            when Int32Col      then col.values.lag(n, default.as?(Int32?))
            when Int64Col      then col.values.lag(n, default.as?(Int64?))
            when BoolCol       then col.values.lag(n, default.as?(Bool?))
            when DateTimeCol   then col.values.lag(n, default.as?(Time?))
            when TimestampCol  then col.values.lag(n, default.as?(Time?))
            when BigDecimalCol then col.values.lag(n, default.as?(BigDecimal?))
            when AnyCol        then col.values.lag(n, default)
            else
              raise InvalidColumnOperationException.new
            end
      Utils.handle_union(Crysda.temp_colname, val)
    end

    def matching(missing_as = false, &) : Array(Bool)
      map { |e| yield e.to_s }.map { |e| e.nil? ? missing_as : e.not_nil! }
    end

    # =========================================================================
    # String convenience methods - delegate to StringCol
    # =========================================================================

    # Check if strings contain a pattern
    def str_contains(pattern : String | Regex, case_sensitive : Bool = true) : Array(Bool)
      case col = self
      when StringCol
        pattern.is_a?(Regex) ? col.contains(pattern) : col.contains(pattern, case_sensitive)
      else
        raise InvalidColumnOperationException.new("str_contains requires StringCol, got #{self.class}")
      end
    end

    # Check if strings start with prefix
    def str_starts_with(prefix : String, case_sensitive : Bool = true) : Array(Bool)
      case col = self
      when StringCol then col.starts_with(prefix, case_sensitive)
      else
        raise InvalidColumnOperationException.new("str_starts_with requires StringCol, got #{self.class}")
      end
    end

    # Check if strings end with suffix
    def str_ends_with(suffix : String, case_sensitive : Bool = true) : Array(Bool)
      case col = self
      when StringCol then col.ends_with(suffix, case_sensitive)
      else
        raise InvalidColumnOperationException.new("str_ends_with requires StringCol, got #{self.class}")
      end
    end

    # Extract regex pattern from strings
    def str_extract(pattern : Regex) : StringCol
      case col = self
      when StringCol then col.extract(pattern)
      else
        raise InvalidColumnOperationException.new("str_extract requires StringCol, got #{self.class}")
      end
    end

    # Replace pattern in strings
    def str_replace(pattern : String | Regex, replacement : String) : StringCol
      case col = self
      when StringCol
        pattern.is_a?(Regex) ? col.replace(pattern, replacement) : col.replace(pattern, replacement)
      else
        raise InvalidColumnOperationException.new("str_replace requires StringCol, got #{self.class}")
      end
    end

    # Convert strings to uppercase
    def str_upcase : StringCol
      case col = self
      when StringCol then col.upcase
      else
        raise InvalidColumnOperationException.new("str_upcase requires StringCol, got #{self.class}")
      end
    end

    # Convert strings to lowercase
    def str_downcase : StringCol
      case col = self
      when StringCol then col.downcase
      else
        raise InvalidColumnOperationException.new("str_downcase requires StringCol, got #{self.class}")
      end
    end

    # Strip whitespace from strings
    def str_strip : StringCol
      case col = self
      when StringCol then col.strip
      else
        raise InvalidColumnOperationException.new("str_strip requires StringCol, got #{self.class}")
      end
    end

    # Get string lengths
    def str_len : Int32Col
      case col = self
      when StringCol then col.len
      else
        raise InvalidColumnOperationException.new("str_len requires StringCol, got #{self.class}")
      end
    end

    # Substring extraction
    def str_slice(start : Int32, length : Int32? = nil) : StringCol
      case col = self
      when StringCol then col.slice(start, length)
      else
        raise InvalidColumnOperationException.new("str_slice requires StringCol, got #{self.class}")
      end
    end

    # =========================================================================
    # Value inspection convenience methods
    # =========================================================================

    # Get unique values in the column
    def unique : Array
      values.compact.uniq
    end

    # Count of unique values
    def nunique : Int32
      unique.size
    end

    # Check if values are in a given set
    def in?(set : Array) : Array(Bool)
      values.map { |v| v.nil? ? false : set.includes?(v) }
    end

    # Check if values are in a given set (alias)
    def is_in(set : Array) : Array(Bool)
      in?(set)
    end

    # Check if numeric values are between min and max (inclusive)
    def between(min_val, max_val) : Array(Bool)
      case col = self
      when Float64Col
        min_f = min_val.to_f64
        max_f = max_val.to_f64
        Array(Bool).new(col.values.size) do |i|
          v = col.unsafe_fetch(i)
          v.nil? ? false : (v >= min_f && v <= max_f)
        end
      when Int32Col
        min_i = min_val.to_i32
        max_i = max_val.to_i32
        Array(Bool).new(col.values.size) do |i|
          v = col.unsafe_fetch(i)
          v.nil? ? false : (v >= min_i && v <= max_i)
        end
      when Int64Col
        min_i = min_val.to_i64
        max_i = max_val.to_i64
        Array(Bool).new(col.values.size) do |i|
          v = col.unsafe_fetch(i)
          v.nil? ? false : (v >= min_i && v <= max_i)
        end
      when BigDecimalCol
        min_bd = BigDecimal.new(min_val.to_f64)
        max_bd = BigDecimal.new(max_val.to_f64)
        Array(Bool).new(col.values.size) do |i|
          v = col.unsafe_fetch(i)
          v.nil? ? false : (v >= min_bd && v <= max_bd)
        end
      else
        raise InvalidColumnOperationException.new("between requires numeric column, got #{self.class}")
      end
    end

    # Clip values to a range
    def clip(min_val, max_val) : DataCol
      case col = self
      when Float64Col
        min_f = min_val.to_f64
        max_f = max_val.to_f64
        result = Slice(Float64).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v < min_f ? min_f : (v > max_f ? max_f : v)
        end
        Float64Col.new(Crysda.temp_colname, result, col.bitmap)
      when Int32Col
        min_i = min_val.to_i32
        max_i = max_val.to_i32
        result = Slice(Int32).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v < min_i ? min_i : (v > max_i ? max_i : v)
        end
        Int32Col.new(Crysda.temp_colname, result, col.bitmap)
      when Int64Col
        min_i = min_val.to_i64
        max_i = max_val.to_i64
        result = Slice(Int64).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v < min_i ? min_i : (v > max_i ? max_i : v)
        end
        Int64Col.new(Crysda.temp_colname, result, col.bitmap)
      when BigDecimalCol
        min_bd = BigDecimal.new(min_val.to_f64)
        max_bd = BigDecimal.new(max_val.to_f64)
        result = Slice(BigDecimal).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v < min_bd ? min_bd : (v > max_bd ? max_bd : v)
        end
        BigDecimalCol.new(Crysda.temp_colname, result, col.bitmap)
      else
        raise InvalidColumnOperationException.new("clip requires numeric column, got #{self.class}")
      end
    end

    # Clip values to minimum
    def clip_lower(min_val) : DataCol
      case col = self
      when Float64Col
        min_f = min_val.to_f64
        result = Slice(Float64).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v < min_f ? min_f : v
        end
        Float64Col.new(Crysda.temp_colname, result, col.bitmap)
      when Int32Col
        min_i = min_val.to_i32
        result = Slice(Int32).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v < min_i ? min_i : v
        end
        Int32Col.new(Crysda.temp_colname, result, col.bitmap)
      when Int64Col
        min_i = min_val.to_i64
        result = Slice(Int64).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v < min_i ? min_i : v
        end
        Int64Col.new(Crysda.temp_colname, result, col.bitmap)
      when BigDecimalCol
        min_bd = BigDecimal.new(min_val.to_f64)
        result = Slice(BigDecimal).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v < min_bd ? min_bd : v
        end
        BigDecimalCol.new(Crysda.temp_colname, result, col.bitmap)
      else
        raise InvalidColumnOperationException.new("clip_lower requires numeric column, got #{self.class}")
      end
    end

    # Clip values to maximum
    def clip_upper(max_val) : DataCol
      case col = self
      when Float64Col
        max_f = max_val.to_f64
        result = Slice(Float64).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v > max_f ? max_f : v
        end
        Float64Col.new(Crysda.temp_colname, result, col.bitmap)
      when Int32Col
        max_i = max_val.to_i32
        result = Slice(Int32).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v > max_i ? max_i : v
        end
        Int32Col.new(Crysda.temp_colname, result, col.bitmap)
      when Int64Col
        max_i = max_val.to_i64
        result = Slice(Int64).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v > max_i ? max_i : v
        end
        Int64Col.new(Crysda.temp_colname, result, col.bitmap)
      when BigDecimalCol
        max_bd = BigDecimal.new(max_val.to_f64)
        result = Slice(BigDecimal).new(col.values.size) do |i|
          v = col.raw_data.unsafe_fetch(i)
          v > max_bd ? max_bd : v
        end
        BigDecimalCol.new(Crysda.temp_colname, result, col.bitmap)
      else
        raise InvalidColumnOperationException.new("clip_upper requires numeric column, got #{self.class}")
      end
    end

    # Forward fill nulls (propagate last valid value)
    def ffill : DataCol
      case col = self
      when Float64Col
        result = Slice(Float64).new(col.values.size, 0.0)
        result_bitmap = NullBitmap.new(col.values.size)
        last_valid_f64 : Float64? = nil
        col.values.size.times do |i|
          if col.bitmap[i]
            if lv = last_valid_f64
              result[i] = lv
            else
              result_bitmap.set(i)
            end
          else
            last_valid_f64 = col.raw_data.unsafe_fetch(i)
            result[i] = last_valid_f64.not_nil!
          end
        end
        Float64Col.new(Crysda.temp_colname, result, result_bitmap)
      when Int32Col
        result = Slice(Int32).new(col.values.size, 0)
        result_bitmap = NullBitmap.new(col.values.size)
        last_valid_i32 : Int32? = nil
        col.values.size.times do |i|
          if col.bitmap[i]
            if lv = last_valid_i32
              result[i] = lv
            else
              result_bitmap.set(i)
            end
          else
            last_valid_i32 = col.raw_data.unsafe_fetch(i)
            result[i] = last_valid_i32.not_nil!
          end
        end
        Int32Col.new(Crysda.temp_colname, result, result_bitmap)
      when Int64Col
        result = Slice(Int64).new(col.values.size, 0_i64)
        result_bitmap = NullBitmap.new(col.values.size)
        last_valid_i64 : Int64? = nil
        col.values.size.times do |i|
          if col.bitmap[i]
            if lv = last_valid_i64
              result[i] = lv
            else
              result_bitmap.set(i)
            end
          else
            last_valid_i64 = col.raw_data.unsafe_fetch(i)
            result[i] = last_valid_i64.not_nil!
          end
        end
        Int64Col.new(Crysda.temp_colname, result, result_bitmap)
      when StringCol
        result = Array(String).new(col.values.size, "")
        result_bitmap = NullBitmap.new(col.values.size)
        last_valid_str : String? = nil
        col.values.size.times do |i|
          if col.bitmap[i]
            if lv = last_valid_str
              result[i] = lv
            else
              result_bitmap.set(i)
            end
          else
            last_valid_str = col.raw_data.unsafe_fetch(i)
            result[i] = last_valid_str.not_nil!
          end
        end
        StringCol.new(Crysda.temp_colname, result, result_bitmap)
      when DateTimeCol
        result_dt = Slice(Int64).new(col.values.size, 0_i64)
        result_bitmap_dt = NullBitmap.new(col.values.size)
        last_dt : Int64? = nil
        col.values.size.times do |i|
          if col.bitmap[i]
            if lv = last_dt
              result_dt[i] = lv
            else
              result_bitmap_dt.set(i)
            end
          else
            last_dt = col.raw_data.unsafe_fetch(i)
            result_dt[i] = last_dt.not_nil!
          end
        end
        DateTimeCol.new(Crysda.temp_colname, result_dt, result_bitmap_dt)
      when TimestampCol
        result_ts = Slice(Int128).new(col.values.size, Int128.new(0))
        result_bitmap_ts = NullBitmap.new(col.values.size)
        last_ts : Int128? = nil
        col.values.size.times do |i|
          if col.bitmap[i]
            if lv = last_ts
              result_ts[i] = lv
            else
              result_bitmap_ts.set(i)
            end
          else
            last_ts = col.raw_data.unsafe_fetch(i)
            result_ts[i] = last_ts.not_nil!
          end
        end
        TimestampCol.new(Crysda.temp_colname, result_ts, result_bitmap_ts)
      when BigDecimalCol
        result_bd = Slice(BigDecimal).new(col.values.size, BigDecimal.new(0))
        result_bitmap_bd = NullBitmap.new(col.values.size)
        last_bd : BigDecimal? = nil
        col.values.size.times do |i|
          if col.bitmap[i]
            if lv = last_bd
              result_bd[i] = lv
            else
              result_bitmap_bd.set(i)
            end
          else
            last_bd = col.raw_data.unsafe_fetch(i)
            result_bd[i] = last_bd.not_nil!
          end
        end
        BigDecimalCol.new(Crysda.temp_colname, result_bd, result_bitmap_bd)
      else
        raise InvalidColumnOperationException.new("ffill not supported for #{self.class}")
      end
    end

    # Backward fill nulls (propagate next valid value)
    def bfill : DataCol
      case col = self
      when Float64Col
        result = Slice(Float64).new(col.values.size, 0.0)
        result_bitmap = NullBitmap.new(col.values.size)
        next_valid_f64 : Float64? = nil
        (col.values.size - 1).downto(0) do |i|
          if col.bitmap[i]
            if nv = next_valid_f64
              result[i] = nv
            else
              result_bitmap.set(i)
            end
          else
            next_valid_f64 = col.raw_data.unsafe_fetch(i)
            result[i] = next_valid_f64.not_nil!
          end
        end
        Float64Col.new(Crysda.temp_colname, result, result_bitmap)
      when Int32Col
        result = Slice(Int32).new(col.values.size, 0)
        result_bitmap = NullBitmap.new(col.values.size)
        next_valid_i32 : Int32? = nil
        (col.values.size - 1).downto(0) do |i|
          if col.bitmap[i]
            if nv = next_valid_i32
              result[i] = nv
            else
              result_bitmap.set(i)
            end
          else
            next_valid_i32 = col.raw_data.unsafe_fetch(i)
            result[i] = next_valid_i32.not_nil!
          end
        end
        Int32Col.new(Crysda.temp_colname, result, result_bitmap)
      when Int64Col
        result = Slice(Int64).new(col.values.size, 0_i64)
        result_bitmap = NullBitmap.new(col.values.size)
        next_valid_i64 : Int64? = nil
        (col.values.size - 1).downto(0) do |i|
          if col.bitmap[i]
            if nv = next_valid_i64
              result[i] = nv
            else
              result_bitmap.set(i)
            end
          else
            next_valid_i64 = col.raw_data.unsafe_fetch(i)
            result[i] = next_valid_i64.not_nil!
          end
        end
        Int64Col.new(Crysda.temp_colname, result, result_bitmap)
      when StringCol
        result = Array(String).new(col.values.size, "")
        result_bitmap = NullBitmap.new(col.values.size)
        next_valid_str : String? = nil
        (col.values.size - 1).downto(0) do |i|
          if col.bitmap[i]
            if nv = next_valid_str
              result[i] = nv
            else
              result_bitmap.set(i)
            end
          else
            next_valid_str = col.raw_data.unsafe_fetch(i)
            result[i] = next_valid_str.not_nil!
          end
        end
        StringCol.new(Crysda.temp_colname, result, result_bitmap)
      when DateTimeCol
        result_bdt = Slice(Int64).new(col.values.size, 0_i64)
        result_bitmap_bdt = NullBitmap.new(col.values.size)
        next_bdt : Int64? = nil
        (col.values.size - 1).downto(0) do |i|
          if col.bitmap[i]
            if nv = next_bdt
              result_bdt[i] = nv
            else
              result_bitmap_bdt.set(i)
            end
          else
            next_bdt = col.raw_data.unsafe_fetch(i)
            result_bdt[i] = next_bdt.not_nil!
          end
        end
        DateTimeCol.new(Crysda.temp_colname, result_bdt, result_bitmap_bdt)
      when TimestampCol
        result_bts = Slice(Int128).new(col.values.size, Int128.new(0))
        result_bitmap_bts = NullBitmap.new(col.values.size)
        next_bts : Int128? = nil
        (col.values.size - 1).downto(0) do |i|
          if col.bitmap[i]
            if nv = next_bts
              result_bts[i] = nv
            else
              result_bitmap_bts.set(i)
            end
          else
            next_bts = col.raw_data.unsafe_fetch(i)
            result_bts[i] = next_bts.not_nil!
          end
        end
        TimestampCol.new(Crysda.temp_colname, result_bts, result_bitmap_bts)
      when BigDecimalCol
        result_bd = Slice(BigDecimal).new(col.values.size, BigDecimal.new(0))
        result_bitmap_bd = NullBitmap.new(col.values.size)
        next_bd : BigDecimal? = nil
        (col.values.size - 1).downto(0) do |i|
          if col.bitmap[i]
            if nv = next_bd
              result_bd[i] = nv
            else
              result_bitmap_bd.set(i)
            end
          else
            next_bd = col.raw_data.unsafe_fetch(i)
            result_bd[i] = next_bd.not_nil!
          end
        end
        BigDecimalCol.new(Crysda.temp_colname, result_bd, result_bitmap_bd)
      else
        raise InvalidColumnOperationException.new("bfill not supported for #{self.class}")
      end
    end

    # Apply a function to each non-null value
    def apply(&block : Any -> Any) : DataCol
      result = values.map { |v| v.nil? ? nil : yield(v) }
      Utils.handle_union(Crysda.temp_colname, result)
    end

    # Bin values into discrete intervals (like pandas cut)
    # bins: Array of bin edges (must be sorted ascending)
    # labels: Optional labels for each bin (size must be bins.size - 1)
    # right: If true (default), bins are (left, right], otherwise [left, right)
    def cut(bins : Array(Number), labels : Array(String)? = nil, right : Bool = true) : StringCol
      raise InvalidColumnOperationException.new("bins must have at least 2 edges") if bins.size < 2
      if labels && labels.size != bins.size - 1
        raise InvalidColumnOperationException.new("labels size (#{labels.size}) must equal bins.size - 1 (#{bins.size - 1})")
      end

      bin_labels = labels || (0...bins.size - 1).map { |i|
        right ? "(#{bins[i]}, #{bins[i + 1]}]" : "[#{bins[i]}, #{bins[i + 1]})"
      }

      result = Array(String?).new(values.size) do |i|
        v = case col = self
            when Float64Col then col.unsafe_fetch(i)
            when Int32Col   then col.unsafe_fetch(i).try(&.to_f64)
            when Int64Col   then col.unsafe_fetch(i).try(&.to_f64)
            else                 nil
            end

        next nil if v.nil?
        val = v.not_nil!

        # Find the bin
        bin_idx : Int32? = nil
        (0...bins.size - 1).each do |j|
          lower = bins[j].to_f64
          upper = bins[j + 1].to_f64
          in_bin = if right
                     val > lower && val <= upper
                   else
                     val >= lower && val < upper
                   end
          if in_bin
            bin_idx = j
            break
          end
        end
        bin_idx ? bin_labels[bin_idx] : nil
      end

      StringCol.new(Crysda.temp_colname, result)
    end

    # Quantile-based binning (like pandas qcut)
    # q: Number of quantiles (e.g., 4 for quartiles, 10 for deciles)
    # labels: Optional labels for each bin
    def qcut(q : Int32, labels : Array(String)? = nil) : StringCol
      raise InvalidColumnOperationException.new("q must be at least 2") if q < 2
      if labels && labels.size != q
        raise InvalidColumnOperationException.new("labels size (#{labels.size}) must equal q (#{q})")
      end

      # Get non-null values and compute quantiles
      vals = case col = self
             when Float64Col then col.values.compact
             when Int32Col   then col.values.compact.map(&.to_f64)
             when Int64Col   then col.values.compact.map(&.to_f64)
             else                 raise InvalidColumnOperationException.new("qcut requires numeric column")
             end

      return StringCol.new(Crysda.temp_colname, Array(String?).new(values.size, nil)) if vals.empty?

      sorted = vals.sort
      n = sorted.size

      # Compute bin edges at quantile boundaries
      bins = Array(Float64).new(q + 1) do |i|
        if i == 0
          sorted.first - 0.001 # Slightly below min to include it
        elsif i == q
          sorted.last
        else
          idx = (n * i / q).to_i
          idx = n - 1 if idx >= n
          sorted[idx]
        end
      end

      cut(bins, labels, right: true)
    end

    # Column-level coalesce - return first non-null from self or other
    def coalesce(other : DataCol) : DataCol
      result = Array(Any).new(values.size) do |i|
        v = self[i]
        v.nil? ? other[i] : v
      end
      Utils.handle_union(Crysda.temp_colname, result)
    end

    def as_s
      case self
      when Int32Col, Int64Col, Float64Col, BoolCol, BigDecimalCol, AnyCol
        Array(String?).new(values.size) { |i| v = values[i]; v.nil? ? nil : v.to_s }
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
      when Int32Col      then Array(Float64?).new(values.size) { |i| self[i].try &.to_f64 }
      when Int64Col      then Array(Float64?).new(values.size) { |i| self[i].try &.to_f64 }
      when BigDecimalCol then Array(Float64?).new(values.size) { |i| self[i].try &.to_f64 }
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

    # =========================================================================
    # Window Functions
    # =========================================================================

    # Rolling mean (moving average) with specified window size
    def rolling_mean(window : Int32, min_periods : Int32? = nil) : Float64Col
      min_p = min_periods || window
      result = Slice(Float64).new(@data.size, 0.0)
      result_bitmap = NullBitmap.new(@data.size)

      @data.size.times do |i|
        if @null_bitmap[i]
          result_bitmap.set(i)
          next
        end

        start_idx = Math.max(0, i - window + 1)
        sum = 0.0
        count = 0

        (start_idx..i).each do |j|
          unless @null_bitmap[j]
            sum += @data.unsafe_fetch(j)
            count += 1
          end
        end

        if count >= min_p
          result[i] = sum / count
        else
          result_bitmap.set(i)
        end
      end

      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    # Rolling sum with specified window size
    def rolling_sum(window : Int32, min_periods : Int32? = nil) : Float64Col
      min_p = min_periods || window
      result = Slice(Float64).new(@data.size, 0.0)
      result_bitmap = NullBitmap.new(@data.size)

      @data.size.times do |i|
        if @null_bitmap[i]
          result_bitmap.set(i)
          next
        end

        start_idx = Math.max(0, i - window + 1)
        sum = 0.0
        count = 0

        (start_idx..i).each do |j|
          unless @null_bitmap[j]
            sum += @data.unsafe_fetch(j)
            count += 1
          end
        end

        if count >= min_p
          result[i] = sum
        else
          result_bitmap.set(i)
        end
      end

      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    # Rolling minimum with specified window size
    def rolling_min(window : Int32, min_periods : Int32? = nil) : Float64Col
      min_p = min_periods || window
      result = Slice(Float64).new(@data.size, 0.0)
      result_bitmap = NullBitmap.new(@data.size)

      @data.size.times do |i|
        if @null_bitmap[i]
          result_bitmap.set(i)
          next
        end

        start_idx = Math.max(0, i - window + 1)
        min_val = Float64::MAX
        count = 0

        (start_idx..i).each do |j|
          unless @null_bitmap[j]
            v = @data.unsafe_fetch(j)
            min_val = v if v < min_val
            count += 1
          end
        end

        if count >= min_p
          result[i] = min_val
        else
          result_bitmap.set(i)
        end
      end

      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    # Rolling maximum with specified window size
    def rolling_max(window : Int32, min_periods : Int32? = nil) : Float64Col
      min_p = min_periods || window
      result = Slice(Float64).new(@data.size, 0.0)
      result_bitmap = NullBitmap.new(@data.size)

      @data.size.times do |i|
        if @null_bitmap[i]
          result_bitmap.set(i)
          next
        end

        start_idx = Math.max(0, i - window + 1)
        max_val = -Float64::MAX
        count = 0

        (start_idx..i).each do |j|
          unless @null_bitmap[j]
            v = @data.unsafe_fetch(j)
            max_val = v if v > max_val
            count += 1
          end
        end

        if count >= min_p
          result[i] = max_val
        else
          result_bitmap.set(i)
        end
      end

      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    # Rolling standard deviation with specified window size
    def rolling_std(window : Int32, min_periods : Int32? = nil) : Float64Col
      min_p = min_periods || window
      result = Slice(Float64).new(@data.size, 0.0)
      result_bitmap = NullBitmap.new(@data.size)

      @data.size.times do |i|
        if @null_bitmap[i]
          result_bitmap.set(i)
          next
        end

        start_idx = Math.max(0, i - window + 1)
        values = Array(Float64).new
        (start_idx..i).each do |j|
          values << @data.unsafe_fetch(j) unless @null_bitmap[j]
        end

        if values.size >= min_p && values.size > 1
          mean = values.sum / values.size
          sum_sq = values.sum { |v| (v - mean) ** 2 }
          result[i] = Math.sqrt(sum_sq / (values.size - 1))
        else
          result_bitmap.set(i)
        end
      end

      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    # Exponential weighted moving average
    def ewm_mean(span : Int32) : Float64Col
      alpha = 2.0 / (span + 1)
      result = Slice(Float64).new(@data.size, 0.0)
      result_bitmap = NullBitmap.new(@data.size)

      ewm = 0.0
      initialized = false

      @data.size.times do |i|
        if @null_bitmap[i]
          result_bitmap.set(i)
        elsif !initialized
          ewm = @data.unsafe_fetch(i)
          result[i] = ewm
          initialized = true
        else
          ewm = alpha * @data.unsafe_fetch(i) + (1 - alpha) * ewm
          result[i] = ewm
        end
      end

      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    # Difference between current and previous value
    def diff(periods : Int32 = 1) : Float64Col
      result = Slice(Float64).new(@data.size, 0.0)
      result_bitmap = NullBitmap.new(@data.size)

      @data.size.times do |i|
        if i < periods || @null_bitmap[i]
          result_bitmap.set(i)
        else
          prev_idx = i - periods
          if @null_bitmap[prev_idx]
            result_bitmap.set(i)
          else
            result[i] = @data.unsafe_fetch(i) - @data.unsafe_fetch(prev_idx)
          end
        end
      end

      Float64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    def to_big_decimal(name : String = @name) : BigDecimalCol
      bd_data = Slice(BigDecimal).new(@data.size) { |i| BigDecimal.new(@data.unsafe_fetch(i)) }
      BigDecimalCol.new(name, bd_data, @null_bitmap)
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

    # =========================================================================
    # Window Functions (delegate to Float64 for precision)
    # =========================================================================

    def rolling_mean(window : Int32, min_periods : Int32? = nil) : Float64Col
      to_f64_col.rolling_mean(window, min_periods)
    end

    def rolling_sum(window : Int32, min_periods : Int32? = nil) : Float64Col
      to_f64_col.rolling_sum(window, min_periods)
    end

    def rolling_min(window : Int32, min_periods : Int32? = nil) : Float64Col
      to_f64_col.rolling_min(window, min_periods)
    end

    def rolling_max(window : Int32, min_periods : Int32? = nil) : Float64Col
      to_f64_col.rolling_max(window, min_periods)
    end

    def rolling_std(window : Int32, min_periods : Int32? = nil) : Float64Col
      to_f64_col.rolling_std(window, min_periods)
    end

    def ewm_mean(span : Int32) : Float64Col
      to_f64_col.ewm_mean(span)
    end

    def diff(periods : Int32 = 1) : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      result_bitmap = NullBitmap.new(@data.size)

      @data.size.times do |i|
        if i < periods || @null_bitmap[i]
          result_bitmap.set(i)
        else
          prev_idx = i - periods
          if @null_bitmap[prev_idx]
            result_bitmap.set(i)
          else
            result[i] = @data.unsafe_fetch(i) - @data.unsafe_fetch(prev_idx)
          end
        end
      end

      Int32Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def to_f64_col : Float64Col
      data = Slice(Float64).new(@data.size) { |i| @data.unsafe_fetch(i).to_f64 }
      Float64Col.new(@name, data, @null_bitmap)
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

    # =========================================================================
    # Window Functions (delegate to Float64 for precision)
    # =========================================================================

    def rolling_mean(window : Int32, min_periods : Int32? = nil) : Float64Col
      to_f64_col.rolling_mean(window, min_periods)
    end

    def rolling_sum(window : Int32, min_periods : Int32? = nil) : Float64Col
      to_f64_col.rolling_sum(window, min_periods)
    end

    def rolling_min(window : Int32, min_periods : Int32? = nil) : Float64Col
      to_f64_col.rolling_min(window, min_periods)
    end

    def rolling_max(window : Int32, min_periods : Int32? = nil) : Float64Col
      to_f64_col.rolling_max(window, min_periods)
    end

    def rolling_std(window : Int32, min_periods : Int32? = nil) : Float64Col
      to_f64_col.rolling_std(window, min_periods)
    end

    def ewm_mean(span : Int32) : Float64Col
      to_f64_col.ewm_mean(span)
    end

    def diff(periods : Int32 = 1) : Int64Col
      result = Slice(Int64).new(@data.size, 0_i64)
      result_bitmap = NullBitmap.new(@data.size)

      @data.size.times do |i|
        if i < periods || @null_bitmap[i]
          result_bitmap.set(i)
        else
          prev_idx = i - periods
          if @null_bitmap[prev_idx]
            result_bitmap.set(i)
          else
            result[i] = @data.unsafe_fetch(i) - @data.unsafe_fetch(prev_idx)
          end
        end
      end

      Int64Col.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def to_f64_col : Float64Col
      data = Slice(Float64).new(@data.size) { |i| @data.unsafe_fetch(i).to_f64 }
      Float64Col.new(@name, data, @null_bitmap)
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

    # =========================================================================
    # String Operations
    # =========================================================================

    # Check if strings contain a pattern (string or regex)
    def contains(pattern : String, case_sensitive : Bool = true) : Array(Bool)
      pat = case_sensitive ? pattern : pattern.downcase
      Array(Bool).new(@data.size) do |i|
        if @null_bitmap[i]
          false
        else
          str = case_sensitive ? @data.unsafe_fetch(i) : @data.unsafe_fetch(i).downcase
          str.includes?(pat)
        end
      end
    end

    # Check if strings contain a regex pattern
    def contains(pattern : Regex) : Array(Bool)
      Array(Bool).new(@data.size) do |i|
        @null_bitmap[i] ? false : pattern.matches?(@data.unsafe_fetch(i))
      end
    end

    # Check if strings start with prefix
    def starts_with(prefix : String, case_sensitive : Bool = true) : Array(Bool)
      pat = case_sensitive ? prefix : prefix.downcase
      Array(Bool).new(@data.size) do |i|
        if @null_bitmap[i]
          false
        else
          str = case_sensitive ? @data.unsafe_fetch(i) : @data.unsafe_fetch(i).downcase
          str.starts_with?(pat)
        end
      end
    end

    # Check if strings end with suffix
    def ends_with(suffix : String, case_sensitive : Bool = true) : Array(Bool)
      pat = case_sensitive ? suffix : suffix.downcase
      Array(Bool).new(@data.size) do |i|
        if @null_bitmap[i]
          false
        else
          str = case_sensitive ? @data.unsafe_fetch(i) : @data.unsafe_fetch(i).downcase
          str.ends_with?(pat)
        end
      end
    end

    # Extract first match of regex pattern, returns StringCol
    # If regex has capture groups, returns the first capture group
    def extract(pattern : Regex) : StringCol
      result = Array(String).new(@data.size, "")
      bitmap = NullBitmap.new(@data.size)

      @data.size.times do |i|
        if @null_bitmap[i]
          bitmap.set(i)
        else
          match = pattern.match(@data.unsafe_fetch(i))
          if match
            # If there are capture groups, return first group, otherwise full match
            result[i] = match[1]? || match[0]
          else
            bitmap.set(i)
          end
        end
      end
      StringCol.new(Crysda.temp_colname, result, bitmap)
    end

    # Replace pattern with replacement string
    def replace(pattern : String, replacement : String) : StringCol
      result = Array(String).new(@data.size) do |i|
        @null_bitmap[i] ? "" : @data.unsafe_fetch(i).gsub(pattern, replacement)
      end
      StringCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Replace regex pattern with replacement string
    def replace(pattern : Regex, replacement : String) : StringCol
      result = Array(String).new(@data.size) do |i|
        @null_bitmap[i] ? "" : @data.unsafe_fetch(i).gsub(pattern, replacement)
      end
      StringCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Convert to uppercase
    def upcase : StringCol
      result = Array(String).new(@data.size) do |i|
        @null_bitmap[i] ? "" : @data.unsafe_fetch(i).upcase
      end
      StringCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Convert to lowercase
    def downcase : StringCol
      result = Array(String).new(@data.size) do |i|
        @null_bitmap[i] ? "" : @data.unsafe_fetch(i).downcase
      end
      StringCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Strip whitespace from both ends
    def strip : StringCol
      result = Array(String).new(@data.size) do |i|
        @null_bitmap[i] ? "" : @data.unsafe_fetch(i).strip
      end
      StringCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Strip whitespace from left
    def lstrip : StringCol
      result = Array(String).new(@data.size) do |i|
        @null_bitmap[i] ? "" : @data.unsafe_fetch(i).lstrip
      end
      StringCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Strip whitespace from right
    def rstrip : StringCol
      result = Array(String).new(@data.size) do |i|
        @null_bitmap[i] ? "" : @data.unsafe_fetch(i).rstrip
      end
      StringCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Get string length
    def len : Int32Col
      result = Slice(Int32).new(@data.size) do |i|
        @null_bitmap[i] ? 0 : @data.unsafe_fetch(i).size
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Substring extraction
    def slice(start : Int32, length : Int32? = nil) : StringCol
      result = Array(String).new(@data.size) do |i|
        if @null_bitmap[i]
          ""
        else
          str = @data.unsafe_fetch(i)
          if len = length
            str[start, len]? || ""
          else
            str[start..]? || ""
          end
        end
      end
      StringCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Pad string to specified width (left pad)
    def pad_left(width : Int32, pad_char : Char = ' ') : StringCol
      result = Array(String).new(@data.size) do |i|
        @null_bitmap[i] ? "" : @data.unsafe_fetch(i).rjust(width, pad_char)
      end
      StringCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Pad string to specified width (right pad)
    def pad_right(width : Int32, pad_char : Char = ' ') : StringCol
      result = Array(String).new(@data.size) do |i|
        @null_bitmap[i] ? "" : @data.unsafe_fetch(i).ljust(width, pad_char)
      end
      StringCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Check if strings match regex exactly
    def matches(pattern : Regex) : Array(Bool)
      Array(Bool).new(@data.size) do |i|
        if @null_bitmap[i]
          false
        else
          match = pattern.match(@data.unsafe_fetch(i))
          match ? match[0] == @data.unsafe_fetch(i) : false
        end
      end
    end

    # Count occurrences of pattern in each string
    def count(pattern : String) : Int32Col
      result = Slice(Int32).new(@data.size) do |i|
        @null_bitmap[i] ? 0 : @data.unsafe_fetch(i).count(pattern)
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
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

  # ===========================================================================
  # DateTime Column - stores Time values with optimized internal storage
  # ===========================================================================
  struct DateTimeCol < DataCol
    @data : Slice(Int64) # Unix epoch milliseconds
    @null_bitmap : NullBitmap
    @cached_values : Array(Time?)?

    # Second-level date/time formats for parsing
    DATETIME_FORMATS = [
      "%Y-%m-%d %H:%M:%S",
      "%Y-%m-%dT%H:%M:%S",
      "%Y-%m-%dT%H:%M:%SZ",
      "%Y-%m-%d %H:%M",
      "%Y-%m-%d",
      "%d/%m/%Y %H:%M:%S",
      "%d/%m/%Y",
      "%m/%d/%Y %H:%M:%S",
      "%m/%d/%Y",
      "%Y%m%d",
    ]

    # Millisecond-level formats (extends DATETIME_FORMATS with %L)
    DATETIME_MS_FORMATS = [
      "%Y-%m-%d %H:%M:%S.%L",
      "%Y-%m-%dT%H:%M:%S.%L",
      "%Y-%m-%dT%H:%M:%S.%LZ",
      "%d/%m/%Y %H:%M:%S.%L",
      "%m/%d/%Y %H:%M:%S.%L",
    ]

    def initialize(@name : String, val : Array(Time?))
      super(@name)
      @data = Slice(Int64).new(val.size, 0_i64)
      @null_bitmap = NullBitmap.new(val.size)
      val.each_with_index do |v, i|
        if v.nil?
          @null_bitmap.set(i)
        else
          @data[i] = v.to_unix_ms
        end
      end
      @cached_values = nil
    end

    protected def initialize(@name : String, @data : Slice(Int64), @null_bitmap : NullBitmap)
      super(@name)
      @cached_values = nil
    end

    # Create from array of epoch seconds (converts to milliseconds internally)
    def self.from_epoch(name : String, epochs : Array(Int64?)) : DateTimeCol
      data = Slice(Int64).new(epochs.size, 0_i64)
      bitmap = NullBitmap.new(epochs.size)
      epochs.each_with_index do |v, i|
        if v.nil?
          bitmap.set(i)
        else
          data[i] = v * 1000
        end
      end
      new(name, data, bitmap)
    end

    # Create from array of epoch milliseconds
    def self.from_epoch_ms(name : String, epochs : Array(Int64?)) : DateTimeCol
      data = Slice(Int64).new(epochs.size, 0_i64)
      bitmap = NullBitmap.new(epochs.size)
      epochs.each_with_index do |v, i|
        if v.nil?
          bitmap.set(i)
        else
          data[i] = v
        end
      end
      new(name, data, bitmap)
    end

    # Parse strings to DateTime using common formats
    def self.parse(name : String, strings : Array(String?), format : String? = nil) : DateTimeCol
      data = Slice(Int64).new(strings.size, 0_i64)
      bitmap = NullBitmap.new(strings.size)

      strings.each_with_index do |s, i|
        if s.nil? || s.empty?
          bitmap.set(i)
        else
          time = parse_datetime(s, format)
          if time.nil?
            bitmap.set(i)
          else
            data[i] = time.to_unix_ms
          end
        end
      end
      new(name, data, bitmap)
    end

    # Try parsing with specified format or auto-detect
    protected def self.parse_datetime(s : String, format : String? = nil) : Time?
      if fmt = format
        Time.parse(s, fmt, Time::Location::UTC) rescue nil
      else
        # Try ms-precision formats first so .123 isn't swallowed by base formats
        DATETIME_MS_FORMATS.each do |fmt|
          begin
            return Time.parse(s, fmt, Time::Location::UTC)
          rescue
            next
          end
        end
        DATETIME_FORMATS.each do |fmt|
          begin
            return Time.parse(s, fmt, Time::Location::UTC)
          rescue
            next
          end
        end
        nil
      end
    end

    @[AlwaysInline]
    def has_nulls? : Bool
      @null_bitmap.any?
    end

    def values : Array(Time?)
      @cached_values ||= Array(Time?).new(@data.size) do |i|
        @null_bitmap[i] ? nil : Time.unix_ms(@data.unsafe_fetch(i))
      end
    end

    # Lazy iteration
    def each(&) : Nil
      @data.size.times do |i|
        yield @null_bitmap[i] ? nil : Time.unix_ms(@data.unsafe_fetch(i))
      end
    end

    def each_with_index(&) : Nil
      @data.size.times do |i|
        yield (@null_bitmap[i] ? nil : Time.unix_ms(@data.unsafe_fetch(i))), i
      end
    end

    def each_non_null(&) : Nil
      unless has_nulls?
        @data.each { |v| yield Time.unix_ms(v) }
        return
      end
      @data.size.times do |i|
        yield Time.unix_ms(@data.unsafe_fetch(i)) unless @null_bitmap[i]
      end
    end

    @[AlwaysInline]
    def unsafe_fetch(index : Int32) : Time?
      @null_bitmap[index] ? nil : Time.unix_ms(@data.unsafe_fetch(index))
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
        a.not_nil! <=> b.not_nil!
      end
    end

    # Comparison operators
    {% for op in %w(> >= < <= ==) %}
    def {{op.id}}(val : Time)
      epoch = val.to_unix_ms
      unless has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} epoch }
      end
      Array(Bool).new(@data.size) { |i| !@null_bitmap[i] && @data.unsafe_fetch(i) {{op.id}} epoch }
    end
    {% end %}

    # Extract year component
    def year : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ms(@data.unsafe_fetch(i)).year unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Extract month component (1-12)
    def month : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ms(@data.unsafe_fetch(i)).month unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Extract day component (1-31)
    def day : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ms(@data.unsafe_fetch(i)).day unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Extract hour component (0-23)
    def hour : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ms(@data.unsafe_fetch(i)).hour unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Extract minute component (0-59)
    def minute : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ms(@data.unsafe_fetch(i)).minute unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Extract second component (0-59)
    def second : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ms(@data.unsafe_fetch(i)).second unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Extract millisecond component (0-999)
    def millisecond : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ms(@data.unsafe_fetch(i)).millisecond unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Extract day of week (0=Sunday, 6=Saturday)
    def day_of_week : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ms(@data.unsafe_fetch(i)).day_of_week.value unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Extract day of year (1-366)
    def day_of_year : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ms(@data.unsafe_fetch(i)).day_of_year unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Get min datetime
    def min(remove_na = false) : Time
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v < result }
        return Time.unix_ms(result)
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
      Time.unix_ms(result)
    end

    # Get max datetime
    def max(remove_na = false) : Time
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v > result }
        return Time.unix_ms(result)
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
      Time.unix_ms(result)
    end

    # Format as strings
    def strftime(format : String) : StringCol
      data = Array(String).new(@data.size, "")
      @data.size.times do |i|
        data[i] = Time.unix_ms(@data.unsafe_fetch(i)).to_s(format) unless @null_bitmap[i]
      end
      StringCol.new(Crysda.temp_colname, data, @null_bitmap)
    end

    def to_timestamp(name : String = @name) : TimestampCol
      ns_data = Slice(Int128).new(@data.size) { |i| @data.unsafe_fetch(i).to_i128 * 1_000_000 }
      TimestampCol.new(name, ns_data, @null_bitmap)
    end
  end

  # ===========================================================================
  # TimestampCol - nanosecond precision time column
  # Stores Unix epoch nanoseconds as Int128
  # ===========================================================================
  struct TimestampCol < DataCol
    @data : Slice(Int128) # Unix epoch nanoseconds
    @null_bitmap : NullBitmap
    @cached_values : Array(Time?)?

    # Nanosecond-level formats for parsing
    NANO_FORMATS = [
      "%Y-%m-%d %H:%M:%S.%N",
      "%Y-%m-%dT%H:%M:%S.%N",
      "%Y-%m-%dT%H:%M:%S.%NZ",
      "%d/%m/%Y %H:%M:%S.%N",
      "%m/%d/%Y %H:%M:%S.%N",
    ]

    def initialize(@name : String, val : Array(Time?))
      super(@name)
      @data = Slice(Int128).new(val.size, Int128.new(0))
      @null_bitmap = NullBitmap.new(val.size)
      val.each_with_index do |v, i|
        if v.nil?
          @null_bitmap.set(i)
        else
          @data[i] = v.to_unix_ns
        end
      end
      @cached_values = nil
    end

    protected def initialize(@name : String, @data : Slice(Int128), @null_bitmap : NullBitmap)
      super(@name)
      @cached_values = nil
    end

    # Create from array of epoch nanoseconds
    def self.from_epoch_ns(name : String, epochs : Array(Int128?)) : TimestampCol
      data = Slice(Int128).new(epochs.size, Int128.new(0))
      bitmap = NullBitmap.new(epochs.size)
      epochs.each_with_index do |v, i|
        if v.nil?
          bitmap.set(i)
        else
          data[i] = v
        end
      end
      new(name, data, bitmap)
    end

    # Parse strings with nanosecond formats
    def self.parse(name : String, strings : Array(String?), format : String? = nil) : TimestampCol
      data = Slice(Int128).new(strings.size, Int128.new(0))
      bitmap = NullBitmap.new(strings.size)

      strings.each_with_index do |s, i|
        if s.nil? || s.empty?
          bitmap.set(i)
        else
          time = parse_timestamp(s, format)
          if time.nil?
            bitmap.set(i)
          else
            data[i] = time.to_unix_ns
          end
        end
      end
      new(name, data, bitmap)
    end

    protected def self.parse_timestamp(s : String, format : String? = nil) : Time?
      if fmt = format
        Time.parse(s, fmt, Time::Location::UTC) rescue nil
      else
        NANO_FORMATS.each do |fmt|
          begin
            return Time.parse(s, fmt, Time::Location::UTC)
          rescue
            next
          end
        end
        nil
      end
    end

    @[AlwaysInline]
    def has_nulls? : Bool
      @null_bitmap.any?
    end

    def values : Array(Time?)
      @cached_values ||= Array(Time?).new(@data.size) do |i|
        @null_bitmap[i] ? nil : Time.unix_ns(@data.unsafe_fetch(i))
      end
    end

    def each(&) : Nil
      @data.size.times do |i|
        yield @null_bitmap[i] ? nil : Time.unix_ns(@data.unsafe_fetch(i))
      end
    end

    def each_with_index(&) : Nil
      @data.size.times do |i|
        yield (@null_bitmap[i] ? nil : Time.unix_ns(@data.unsafe_fetch(i))), i
      end
    end

    def each_non_null(&) : Nil
      unless has_nulls?
        @data.each { |v| yield Time.unix_ns(v) }
        return
      end
      @data.size.times do |i|
        yield Time.unix_ns(@data.unsafe_fetch(i)) unless @null_bitmap[i]
      end
    end

    @[AlwaysInline]
    def unsafe_fetch(index : Int32) : Time?
      @null_bitmap[index] ? nil : Time.unix_ns(@data.unsafe_fetch(index))
    end

    protected def raw_data : Slice(Int128)
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
        a.not_nil! <=> b.not_nil!
      end
    end

    # Comparison operators
    {% for op in %w(> >= < <= ==) %}
    def {{op.id}}(val : Time)
      epoch = val.to_unix_ns
      unless has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} epoch }
      end
      Array(Bool).new(@data.size) { |i| !@null_bitmap[i] && @data.unsafe_fetch(i) {{op.id}} epoch }
    end
    {% end %}

    # Extract year component
    def year : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ns(@data.unsafe_fetch(i)).year unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    def month : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ns(@data.unsafe_fetch(i)).month unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    def day : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ns(@data.unsafe_fetch(i)).day unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    def hour : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ns(@data.unsafe_fetch(i)).hour unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    def minute : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ns(@data.unsafe_fetch(i)).minute unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    def second : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ns(@data.unsafe_fetch(i)).second unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    def millisecond : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ns(@data.unsafe_fetch(i)).millisecond unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    def microsecond : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ns(@data.unsafe_fetch(i)).nanosecond // 1000 unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    def nanosecond : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ns(@data.unsafe_fetch(i)).nanosecond unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    def day_of_week : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ns(@data.unsafe_fetch(i)).day_of_week.value unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    def day_of_year : Int32Col
      result = Slice(Int32).new(@data.size, 0)
      @data.size.times do |i|
        result[i] = Time.unix_ns(@data.unsafe_fetch(i)).day_of_year unless @null_bitmap[i]
      end
      Int32Col.new(Crysda.temp_colname, result, @null_bitmap)
    end

    # Get min timestamp
    def min(remove_na = false) : Time
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v < result }
        return Time.unix_ns(result)
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      result = Int128::MAX
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
      Time.unix_ns(result)
    end

    # Get max timestamp
    def max(remove_na = false) : Time
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v > result }
        return Time.unix_ns(result)
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      result = Int128::MIN
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
      Time.unix_ns(result)
    end

    # Format as strings
    def strftime(format : String) : StringCol
      data = Array(String).new(@data.size, "")
      @data.size.times do |i|
        data[i] = Time.unix_ns(@data.unsafe_fetch(i)).to_s(format) unless @null_bitmap[i]
      end
      StringCol.new(Crysda.temp_colname, data, @null_bitmap)
    end

    def to_datetime(name : String = @name) : DateTimeCol
      ms_data = Slice(Int64).new(@data.size) { |i| (@data.unsafe_fetch(i) / 1_000_000).to_i64 }
      DateTimeCol.new(name, ms_data, @null_bitmap)
    end
  end

  # ===========================================================================
  # BigDecimal Column - high precision decimal storage
  # ===========================================================================
  struct BigDecimalCol < DataCol
    @data : Slice(BigDecimal)
    @null_bitmap : NullBitmap
    @cached_values : Array(BigDecimal?)?

    def initialize(@name : String, val : Array(BigDecimal?))
      super(@name)
      @data = Slice(BigDecimal).new(val.size, BigDecimal.new(0))
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

    protected def initialize(@name : String, @data : Slice(BigDecimal), @null_bitmap : NullBitmap)
      super(@name)
      @cached_values = nil
    end

    @[AlwaysInline]
    def has_nulls? : Bool
      @null_bitmap.any?
    end

    def values : Array(BigDecimal?)
      @cached_values ||= Array(BigDecimal?).new(@data.size) do |i|
        @null_bitmap[i] ? nil : @data.unsafe_fetch(i)
      end
    end

    def each(&) : Nil
      @data.size.times do |i|
        yield @null_bitmap[i] ? nil : @data.unsafe_fetch(i)
      end
    end

    def each_with_index(&) : Nil
      @data.size.times do |i|
        yield (@null_bitmap[i] ? nil : @data.unsafe_fetch(i)), i
      end
    end

    def each_non_null(&) : Nil
      unless has_nulls?
        @data.each { |v| yield v }
        return
      end
      @data.size.times do |i|
        yield @data.unsafe_fetch(i) unless @null_bitmap[i]
      end
    end

    @[AlwaysInline]
    def unsafe_fetch(index : Int32) : BigDecimal?
      @null_bitmap[index] ? nil : @data.unsafe_fetch(index)
    end

    protected def raw_data : Slice(BigDecimal)
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

    def opt_sum(remove_na = false) : BigDecimal
      unless has_nulls?
        total = BigDecimal.new(0)
        @data.each { |v| total += v }
        return total
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      total = BigDecimal.new(0)
      @data.size.times { |i| total += @data.unsafe_fetch(i) unless @null_bitmap[i] }
      total
    end

    def opt_mean(remove_na = false) : BigDecimal
      unless has_nulls?
        total = BigDecimal.new(0)
        @data.each { |v| total += v }
        return total / @data.size
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      total = BigDecimal.new(0)
      count = 0
      @data.size.times do |i|
        unless @null_bitmap[i]
          total += @data.unsafe_fetch(i)
          count += 1
        end
      end
      total / count
    end

    def opt_min(remove_na = false) : BigDecimal
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v < result }
        return result
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      found = false
      result = BigDecimal.new(0)
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

    def opt_max(remove_na = false) : BigDecimal
      unless has_nulls?
        result = @data[0]
        @data.each { |v| result = v if v > result }
        return result
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      found = false
      result = BigDecimal.new(0)
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

    def opt_median(remove_na = false) : BigDecimal
      unless has_nulls?
        sorted = @data.to_a.sort!
        mid = sorted.size // 2
        return sorted.size.odd? ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      arr = Array(BigDecimal).new(@data.size - @null_bitmap.count)
      @data.size.times { |i| arr << @data.unsafe_fetch(i) unless @null_bitmap[i] }
      arr.sort!
      mid = arr.size // 2
      arr.size.odd? ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2
    end

    def opt_sd(remove_na = false) : BigDecimal
      m = opt_mean(remove_na)
      unless has_nulls?
        sum_sq = BigDecimal.new(0)
        @data.each { |v| sum_sq += (v - m) ** 2 }
        return BigDecimal.new(Math.sqrt(sum_sq.to_f64 / (@data.size - 1)))
      end
      raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument") unless remove_na
      sum_sq = BigDecimal.new(0)
      count = 0
      @data.size.times do |i|
        unless @null_bitmap[i]
          sum_sq += (@data.unsafe_fetch(i) - m) ** 2
          count += 1
        end
      end
      BigDecimal.new(Math.sqrt(sum_sq.to_f64 / (count - 1)))
    end

    def plus(val)
      case val
      when BigDecimalCol then add_col(val)
      when Number
        add_scalar(BigDecimal.new(val.to_f64))
      else
        raise UnSupportedOperationException.new
      end
    end

    def minus(val)
      case val
      when BigDecimalCol then sub_col(val)
      when Number
        sub_scalar(BigDecimal.new(val.to_f64))
      else
        raise UnSupportedOperationException.new
      end
    end

    def div(val)
      case val
      when BigDecimalCol then div_col(val)
      when Number
        div_scalar(BigDecimal.new(val.to_f64))
      else
        raise UnSupportedOperationException.new
      end
    end

    def times(val)
      case val
      when BigDecimalCol then mul_col(val)
      when Number
        mul_scalar(BigDecimal.new(val.to_f64))
      else
        raise UnSupportedOperationException.new
      end
    end

    private def add_col(other : BigDecimalCol) : BigDecimalCol
      unless has_nulls? || other.has_nulls?
        result = Slice(BigDecimal).new(@data.size) { |i| @data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i) }
        return BigDecimalCol.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(BigDecimal).new(@data.size) { |i| result_bitmap[i] ? BigDecimal.new(0) : @data.unsafe_fetch(i) + other.raw_data.unsafe_fetch(i) }
      BigDecimalCol.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def sub_col(other : BigDecimalCol) : BigDecimalCol
      unless has_nulls? || other.has_nulls?
        result = Slice(BigDecimal).new(@data.size) { |i| @data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i) }
        return BigDecimalCol.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(BigDecimal).new(@data.size) { |i| result_bitmap[i] ? BigDecimal.new(0) : @data.unsafe_fetch(i) - other.raw_data.unsafe_fetch(i) }
      BigDecimalCol.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def mul_col(other : BigDecimalCol) : BigDecimalCol
      unless has_nulls? || other.has_nulls?
        result = Slice(BigDecimal).new(@data.size) { |i| @data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i) }
        return BigDecimalCol.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(BigDecimal).new(@data.size) { |i| result_bitmap[i] ? BigDecimal.new(0) : @data.unsafe_fetch(i) * other.raw_data.unsafe_fetch(i) }
      BigDecimalCol.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def div_col(other : BigDecimalCol) : BigDecimalCol
      unless has_nulls? || other.has_nulls?
        result = Slice(BigDecimal).new(@data.size) { |i| @data.unsafe_fetch(i) / other.raw_data.unsafe_fetch(i) }
        return BigDecimalCol.new(Crysda.temp_colname, result, NullBitmap.none(@data.size))
      end
      result_bitmap = @null_bitmap | other.bitmap
      result = Slice(BigDecimal).new(@data.size) { |i| result_bitmap[i] ? BigDecimal.new(0) : @data.unsafe_fetch(i) / other.raw_data.unsafe_fetch(i) }
      BigDecimalCol.new(Crysda.temp_colname, result, result_bitmap)
    end

    private def add_scalar(scalar : BigDecimal) : BigDecimalCol
      result = Slice(BigDecimal).new(@data.size) { |i| @data.unsafe_fetch(i) + scalar }
      BigDecimalCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def sub_scalar(scalar : BigDecimal) : BigDecimalCol
      result = Slice(BigDecimal).new(@data.size) { |i| @data.unsafe_fetch(i) - scalar }
      BigDecimalCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def mul_scalar(scalar : BigDecimal) : BigDecimalCol
      result = Slice(BigDecimal).new(@data.size) { |i| @data.unsafe_fetch(i) * scalar }
      BigDecimalCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    private def div_scalar(scalar : BigDecimal) : BigDecimalCol
      result = Slice(BigDecimal).new(@data.size) { |i| @data.unsafe_fetch(i) / scalar }
      BigDecimalCol.new(Crysda.temp_colname, result, @null_bitmap)
    end

    {% for op in %w(> >= < <= ==) %}
    def {{op.id}}(val : Number)
      unless has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} BigDecimal.new(val.to_f64) }
      end
      Array(Bool).new(@data.size) { |i| !@null_bitmap[i] && @data.unsafe_fetch(i) {{op.id}} BigDecimal.new(val.to_f64) }
    end

    def {{op.id}}(val : BigDecimalCol)
      unless has_nulls? || val.has_nulls?
        return Array(Bool).new(@data.size) { |i| @data.unsafe_fetch(i) {{op.id}} val.raw_data.unsafe_fetch(i) }
      end
      Array(Bool).new(@data.size) do |i|
        !@null_bitmap[i] && !val.bitmap[i] && @data.unsafe_fetch(i) {{op.id}} val.raw_data.unsafe_fetch(i)
      end
    end
    {% end %}

    def to_f64(name : String = @name) : Float64Col
      f64_data = Slice(Float64).new(@data.size) { |i| @data.unsafe_fetch(i).to_f64 }
      Float64Col.new(name, f64_data, @null_bitmap)
    end
  end
end
