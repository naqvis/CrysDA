module Crysda
  # Wrapper around `Any` and provide convenient methods like `as_xxx` to access the value in specific format.
  struct AnyVal
    getter raw : Any | DataFrame

    def initialize(@raw)
    end

    def self.[](val : Any | DataFrame)
      new(val)
    end

    # Checks that the underlying value is `Bool`, and returns its value.
    # Raises otherwise.
    def as_bool : Bool
      @raw.as(Bool)
    end

    # Checks that the underlying value is `Bool`, and returns its value.
    # Returns `nil` otherwise.
    def as_bool? : Bool?
      as_bool if @raw.is_a?(Bool)
    end

    # Checks that the underlying value is `Int`, and returns its value as an `Int32`.
    # Raises otherwise.
    def as_i : Int32
      @raw.as(Int).to_i
    end

    # Checks that the underlying value is `Int`, and returns its value as an `Int32`.
    # Returns `nil` otherwise.
    def as_i? : Int32?
      as_i if @raw.is_a?(Int)
    end

    # Checks that the underlying value is `Int`, and returns its value as an `Int64`.
    # Raises otherwise.
    def as_i64 : Int64
      @raw.as(Int).to_i64
    end

    # Checks that the underlying value is `Int`, and returns its value as an `Int64`.
    # Returns `nil` otherwise.
    def as_i64? : Int64?
      as_i64 if @raw.is_a?(Int64)
    end

    # Checks that the underlying value is `Float`, and returns its value as an `Float64`.
    # Raises otherwise.
    def as_f : Float64
      @raw.as(Float64)
    end

    # Checks that the underlying value is `Float`, and returns its value as an `Float64`.
    # Returns `nil` otherwise.
    def as_f? : Float64?
      @raw.as?(Float64)
    end

    # Checks that the underlying value is `Float`, and returns its value as an `Float32`.
    # Raises otherwise.
    def as_f32 : Float32
      @raw.as(Float).to_f32
    end

    # Checks that the underlying value is `Float`, and returns its value as an `Float32`.
    # Returns `nil` otherwise.
    def as_f32? : Float32?
      as_f32 if @raw.is_a?(Float)
    end

    # Checks that the underlying value is `String`, and returns its value.
    # Raises otherwise.
    def as_s : String
      @raw.as(String)
    end

    # Checks that the underlying value is `String`, and returns its value.
    # Returns `nil` otherwise.
    def as_s? : String?
      as_s if @raw.is_a?(String)
    end

    # Checks that the underlying value is `DataFrame`, and returns its value.
    # Raises otherwise.
    def as_df : DataFrame
      @raw.as(DataFrame)
    end

    # Checks that the underlying value is `DataFrame`, and returns its value.
    # Returns `nil` otherwise.
    def as_df? : DataFrame?
      as_s if @raw.is_a?(DataFrame)
    end

    # Checks that the underlying value is `Nil`, and returns `nil`.
    # Raises otherwise.
    def as_nil : Nil
      @raw.as(Nil)
    end

    # Checks that the underlying value is `Time`, and returns its value.
    # Raises otherwise.
    def as_t : Time
      @raw.as(Time)
    end

    # Checks that the underlying value is `Time`, and returns its value.
    # Returns `nil` otherwise.
    def as_t? : Time?
      as_s if @raw.is_a?(Time)
    end

    def to_s
      @raw.nil? ? MISSING_VALUE : @raw.to_s
    end

    def to_s(io : IO) : Nil
      io << to_s
    end

    def_equals_and_hash @raw
    forward_missing_to @raw
  end

  private module NAOps(T)
    def self.na_aware_op(first : T?, second : T?, op : (T, T) -> T)
      first.nil? || second.nil? ? nil : op.call(first.not_nil!, second.not_nil!)
    end

    def self.filter_not_nil(arr : Array(T?))
      res = Array(T).new
      arr.each { |a| res << a unless a.nil? }
      res
    end

    def self.force_not_nil!(arr : Array(T?))
      res = Array(T).new
      arr.each do |a|
        if a.nil?
          raise MissingValueException.new("Missing values in data. Consider to use `remove_na` argument or `DataCol#ignore_na()`")
        end
        res << a unless a.nil?
      end
      res
    end
  end

  private module Utils
    extend self

    module Type(T)
      def self.of_type?(items : Array)
        items.each do |v|
          return false unless v.is_a?(T)
        end
        true
      end

      def self.of_type?(items)
        false
      end
    end

    def handle_union(col : DataCol, name : String, arr : Array)
      case (col)
      when Int32Col
        Int32Col.new(name, Array(Int32?).new(arr.size) { |i| arr[i].as?(Int32) })
      when Int64Col
        Int64Col.new(name, Array(Int64?).new(arr.size) { |i| arr[i].as?(Int64) })
      when StringCol
        StringCol.new(name, Array(String?).new(arr.size) { |i| arr[i].as?(String) })
      when Float64Col
        Float64Col.new(name, Array(Float64?).new(arr.size) { |i| arr[i].as?(Float64) })
      when BoolCol
        BoolCol.new(name, Array(Bool?).new(arr.size) { |i| arr[i].as?(Bool) })
      when DFCol
        DFCol.new(name, Array(DataFrame?).new(arr.size) { |i| arr[i].as?(DataFrame) })
      else
        AnyCol.new(name, Array(Any).new(arr.size) { |i| arr[i].as?(Any) })
      end
    end

    def handle_union(name : String, arr)
      arr = arr || Array(Int32).new
      case
      when Type(Int32?).of_type?(arr)
        Int32Col.new(name, Array(Int32?).new(arr.size) { |i| arr[i].as?(Int32) })
      when Type(Int64?).of_type?(arr)
        Int64Col.new(name, Array(Int64?).new(arr.size) { |i| arr[i].as?(Int64) })
      when Type(String?).of_type?(arr)
        StringCol.new(name, Array(String?).new(arr.size) { |i| arr[i].as?(String) })
      when Type(Float64?).of_type?(arr)
        Float64Col.new(name, Array(Float64?).new(arr.size) { |i| arr[i].as?(Float64) })
      when Type(Bool?).of_type?(arr)
        BoolCol.new(name, Array(Bool?).new(arr.size) { |i| arr[i].as?(Bool) })
      when arr.size == 0
        AnyCol.new(name, [] of Any)
      when Type(DataFrame).of_type?(arr)
        DFCol.new(name, Array(DataFrame?).new(arr.size) { |i| arr[i].as?(DataFrame) })
      when Type(Any).of_type?(arr)
        AnyCol.new(name, Array(Any).new(arr.size) { |i| arr[i].as?(Any) })
      else
        raise CrysdaException.new ("Unsupported Operation for value: #{arr} - #{arr.class}")
      end
    end

    def any_as_column(mutation, name : String, nrow : Int32) : DataCol
      arrified_mutation = case (mutation)
                          when Int32   then Array(Int32).new(nrow, mutation)
                          when Int64   then Array(Int64).new(nrow, mutation)
                          when Float32 then Array(Float64).new(nrow, mutation.to_f64)
                          when Float64 then Array(Float64).new(nrow, mutation)
                          when Bool    then Array(Bool).new(nrow, mutation)
                          when String  then Array(String).new(nrow, mutation)
                          else
                            mutation
                          end

      case (arrified_mutation)
      when DataCol
        case (arrified_mutation)
        when Float64Col then Float64Col.new(name, arrified_mutation.values)
        when Int32Col   then Int32Col.new(name, arrified_mutation.values)
        when Int64Col   then Int64Col.new(name, arrified_mutation.values)
        when StringCol  then StringCol.new(name, arrified_mutation.values)
        when BoolCol    then BoolCol.new(name, arrified_mutation.values)
        when AnyCol     then AnyCol.new(name, arrified_mutation.values)
        else
          raise UnSupportedOperationException.new
        end
      when Array(Float32), Array(Float32?)     then Float64Col.new(name, Array(Float64?).new(arrified_mutation.size) { |i| arrified_mutation[i].try &.to_f64 })
      when Array(Float64), Array(Float64?)     then Float64Col.new(name, Array(Float64?).new(arrified_mutation.size) { |i| arrified_mutation[i] })
      when Array(Int32), Array(Int32?)         then Int32Col.new(name, Array(Int32?).new(arrified_mutation.size) { |i| arrified_mutation[i] })
      when Array(Int64), Array(Int64?)         then Int64Col.new(name, Array(Int64?).new(arrified_mutation.size) { |i| arrified_mutation[i] })
      when Array(Bool), Array(Bool?)           then BoolCol.new(name, Array(Bool?).new(arrified_mutation.size) { |i| arrified_mutation[i] })
      when Array(String), Array(String?)       then StringCol.new(name, Array(String?).new(arrified_mutation.size) { |i| arrified_mutation[i] })
      when Array(CustomColumnValue)            then AnyCol.new(name, Array(Any).new(arrified_mutation.size) { |i| arrified_mutation[i] })
      when Array(DataFrame), Array(DataFrame?) then handle_union(name, arrified_mutation)
      when Array(Any)
        if arrified_mutation.size == 0
          AnyCol.new(name, [] of Any)
        else
          handle_union(name, arrified_mutation)
        end
      else
        # raise CrysdaException.new ("Uknown arrified mutation type: #{arrified_mutation}")
        AnyCol.new(name, Array(Any).new(nrow, arrified_mutation))
      end
    end

    def create_value_printer(max_digits = 3)
      ->(val : Any | AnyVal | DataCol | DataFrame) {
        case v = val
        when Float     then v.format(decimal_places: max_digits)
        when DataFrame then "<DataFrame [#{v.num_row} x #{v.num_col}]>"
        when Nil       then "<NA>"
        when String    then v
        when AnyVal
          case (a = v.raw)
          when Float     then a.format(decimal_places: max_digits)
          when DataFrame then "<DataFrame [#{a.num_row} x #{a.num_col}]>"
          when .nil?     then "<NA>"
          else
            a.to_s
          end
        else
          # raise CrysdaException.new ("uknown type: #{v.class}")
          v.to_s
        end
      }
    end

    def get_col(name, values, true_vals = ["T", "TRUE"], false_vals = ["F", "FALSE"])
      elems = values.first(20)
      t_vals = true_vals.map(&.upcase)
      f_vals = false_vals.map(&.upcase)
      case
      when int32col?(elems)
        begin
          Int32Col.new(name, Array(Int32?).new(values.size) { |i| values[i].try &.to_i32 })
        rescue ex : ArgumentError
          col = get_i64col(name, values) || get_f64col(name, values)
          col || StringCol.new(name, values)
        end
      when int64col?(elems)                then get_i64col(name, values) || get_f64col(name, values) || StringCol.new(name, values)
      when float64col?(elems)              then get_f64col(name, values) || StringCol.new(name, values)
      when boolcol?(elems, t_vals, f_vals) then BoolCol.new(name, Array(Bool?).new(values.size) { |i| as_bool?(values[i], t_vals, f_vals) })
      else
        StringCol.new(name, values)
      end
    end

    def get_col(col : DataCol)
      name = col.name
      values = col.as_s
      elems = values.first(20)
      t_vals = ["T", "TRUE"]
      f_vals = ["F", "FALSE"]
      case
      when int32col?(elems)                then Int32Col.new(name, Array(Int32?).new(values.size) { |i| values[i].try &.to_i32 })
      when int64col?(elems)                then Int64Col.new(name, Array(Int64?).new(values.size) { |i| values[i].try &.to_i64 })
      when float64col?(elems)              then Float64Col.new(name, Array(Float64?).new(values.size) { |i| values[i].try &.to_f })
      when boolcol?(elems, t_vals, f_vals) then BoolCol.new(name, Array(Bool?).new(values.size) { |i| as_bool?(values[i], t_vals, f_vals) })
      else
        col
      end
    end

    private def get_i64col(name, values)
      Int64Col.new(name, Array(Int64?).new(values.size) { |i| values[i].try &.to_i64 }) rescue nil
    end

    private def get_f64col(name, values)
      Float64Col.new(name, Array(Float64?).new(values.size) do |i|
        num = values[i]
        if num
          num = num.gsub(',', "")
          num.to_f
        else
          nil
        end
      end)
    rescue
      nil
    end

    private def int32col?(elems)
      elems.map { |v| v.try &.to_i }
      true
    rescue
      false
    end

    private def int64col?(elems)
      elems.map { |v| v.try &.to_i64 }
      true
    rescue
      false
    end

    private def float64col?(elems)
      elems.map { |v| v.try &.to_f }
      true
    rescue
      false
    end

    private def boolcol?(elems, t_vals, f_vals)
      elems.map { |v| as_bool?(v, t_vals, f_vals) }
      true
    rescue
      false
    end

    private def as_bool?(val, t_vals, f_vals)
      return nil if val.nil?
      if (cval = val)
        cval = cval.upcase
        return true if cval.in?(t_vals)
        return false if cval.in?(f_vals)
      end
      raise CrysdaException.new ("invalid boolean conversion")
    end
  end

  # Custom Hashing helper. This provides a calculated hash of contents which is consistent on each and every run. This doesn't conflict with and/or override  Crystal Hashing functionality which is used for object equality checks.
  # Hash value generated by this builder is only used for internal grouping purposes to ensure the consistency on each and every run/invocation.
  # Refer to `CustomColumnValue` for more details and usage purposes.
  struct HashBuilder
    private HASH_BITS    = 61
    private HASH_MODULUS = (1_i64 << HASH_BITS) - 1

    private HASH_NAN       =      0_u64
    private HASH_INF_PLUS  = 314159_u64
    private HASH_INF_MINUS = (-314159_i64).unsafe_as(UInt64)

    def initialize
      @total = 17_i64
      @const = 37_i64
    end

    def add(val : Bool)
      @total = @total &* @const &+ (val ? 0 : 1)
      self
    end

    def add(val : Char)
      add(val.ord)
    end

    def add(val : String)
      add(val.chars)
    end

    def add(val : AnyVal)
      add(val.raw)
    end

    def add(val : Nil)
      @total = Int64::MAX - 123000
      self
    end

    def add(val : Iterable)
      val.each do |v|
        add(v)
      end
      self
    end

    def add(value : Float32)
      normalized_hash = float_normalize_wrap(value) do |val|
        # This optimized version works on every architecture where endianess
        # of Float32 and Int32 matches and float is IEEE754. All supported
        # architectures fall into this category.
        unsafe_int = val.unsafe_as(Int32)
        exp = (((unsafe_int >> 23) & 0xff) - 127)
        mantissa = unsafe_int & ((1 << 23) - 1)
        if exp > -127
          exp -= 23
          mantissa |= 1 << 23
        else
          # subnormals
          exp -= 22
        end
        {mantissa.to_i64, exp}
      end
      add(normalized_hash)
    end

    def add(value : Float64)
      normalized_hash = float_normalize_wrap(value) do |val|
        # This optimized version works on every architecture where endianess
        # of Float64 and Int64 matches and float is IEEE754. All supported
        # architectures fall into this category.
        unsafe_int = val.unsafe_as(Int64)
        exp = (((unsafe_int >> 52) & 0x7ff) - 1023)
        mantissa = unsafe_int & ((1_u64 << 52) - 1)
        if exp > -1023
          exp -= 52
          mantissa |= 1_u64 << 52
        else
          # subnormals
          exp -= 51
        end

        {mantissa.to_i64, exp}
      end
      add(normalized_hash)
    end

    def add(value : Float)
      normalized_hash = float_normalize_wrap(value) do |val|
        frac, exp = Math.frexp val
        float_normalize_reference(val, frac, exp)
      end
      add(normalized_hash)
    end

    def add(val : UUID)
      add(val.to_s)
    end

    def add(val : CustomColumnValue)
      add(val.hashcode)
    end

    def add(val : Int)
      @total = @total &* @const &+ val
      self
    end

    def add(val)
      add(val.hash)
    end

    def hashcode
      @total
    end

    def hashcode(val)
      add(val)
      ret = @total
      reset
      ret
    end

    private def reset
      @total = 17_i64
      @const = 37_i64
    end

    private def float_normalize_reference(value, frac, exp)
      if value < 0
        frac = -frac
      end
      # process 28 bits at a time;  this should work well both for binary
      # and hexadecimal floating point.
      x = 0_i64
      while frac > 0
        x = ((x << 28) & HASH_MODULUS) | x >> (HASH_BITS - 28)
        frac *= 268435456.0 # 2**28
        exp -= 28
        y = frac.to_u32 # pull out integer part
        frac -= y
        x += y
        x -= HASH_MODULUS if x >= HASH_MODULUS
      end
      {x, exp}
    end

    private def float_normalize_wrap(value)
      return HASH_NAN if value.nan?
      if value.infinite?
        return value > 0 ? HASH_INF_PLUS : HASH_INF_MINUS
      end

      x, exp = yield value

      # adjust for the exponent;  first reduce it modulo HASH_BITS
      exp = exp >= 0 ? exp % HASH_BITS : HASH_BITS - 1 - ((-1 - exp) % HASH_BITS)
      x = ((x << exp) & HASH_MODULUS) | x >> (HASH_BITS - exp)

      (x * (value < 0 ? -1 : 1)).to_i64.unsafe_as(UInt64)
    end
  end
end
