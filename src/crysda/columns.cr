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

  # Helper module to provide common aggregate methods to be used along `DataFrame#summarize_at`
  module AggFuncs
    extend self

    def mean
      AggFunc.new(SumFormula.new { |c| c.mean }, "mean")
    end

    def median
      AggFunc.new(SumFormula.new { |c| c.median }, "median")
    end

    def sd
      AggFunc.new(SumFormula.new { |c| c.sd }, "sd")
    end

    def n
      AggFunc.new(SumFormula.new { |c| c.size }, "n")
    end

    def na
      AggFunc.new(SumFormula.new { |c| c.is_na.filter { |_| true }.size }, "na")
    end
  end

  # Abstract base struct for column types of Int32?, Int64?, Float64?, Bool?, String?, DataFrame? or Any type
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
      res = case (self)
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

    # `order` returns the index each element would have in an ascending list
    # ```
    # y = Float64Col.new("foo", [3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1])
    # y.order # => [8, 1, 3, 9, 2, 6, 7, 0, 4, 5]
    # ```
    def order(na_last = true)
      (0..(values.size - 1)).to_a.sort { |a, b| self.compare(a, b, na_last) }
    end

    # `rank` returns the order of each element in an ascending list
    # ```
    # y = Float64Col.new("foo", [3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1])
    # y.rank # => [7, 1, 4, 2, 8, 9, 5, 6, 0, 3]
    # ```
    def rank(na_last = true)
      order(na_last).map_with_index { |v, i| {i, v} }.sort_by { |a| a[1] }.map(&.[0])
    end

    # Creates a sorting attribute that inverts the order of the argument
    def desc
      Int32Col.new(Crysda.temp_colname, rank(false).map(&.-))
    end

    def has_nulls?
      values.any? { |v| v.nil? }
    end

    def map(&block)
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

    # Maps a column to true for NA values and `false` otherwise
    # Returns a `Bool` array with NA values marked as true, while others are marked as false
    def is_na
      values.map(&.nil?)
    end

    # Maps a column to false for NA values and `true` otherwise
    # Returns a `Bool` array with NA values marked as false, while others are marked as true
    def is_not_na
      values.map { |v| !v.nil? }
    end

    # Calculates the minimum of the column values.
    # remove_na If `true` missing values will be excluded from the operation
    # raises `MissingValueException` if `remove_na` is `false` but the data contains missing values
    # raises `InvalidColumnOperationException` If the type of the column is not numeric
    def min(remove_na = false)
      case e = self
      when Int32Col
        remove_na ? NAOps(Int32).filter_not_nil(e.values) : NAOps(Int32).force_not_nil!(e.values)
      when Int64Col
        remove_na ? NAOps(Int64).filter_not_nil(e.values) : NAOps(Int64).force_not_nil!(e.values)
      when Float64Col
        remove_na ? NAOps(Float64).filter_not_nil(e.values) : NAOps(Float64).force_not_nil!(e.values)
      else
        raise InvalidColumnOperationException.new
      end.min
    end

    # Calculates the maximum of the column values.
    # remove_na If `true` missing values will be excluded from the operation
    # raises `MissingValueException` if `remove_na` is `false` but the data contains missing values
    # raises `InvalidColumnOperationException` If the type of the column is not numeric
    def max(remove_na = false)
      case e = self
      when Int32Col
        remove_na ? NAOps(Int32).filter_not_nil(e.values) : NAOps(Int32).force_not_nil!(e.values)
      when Int64Col
        remove_na ? NAOps(Int64).filter_not_nil(e.values) : NAOps(Int64).force_not_nil!(e.values)
      when Float64Col
        remove_na ? NAOps(Float64).filter_not_nil(e.values) : NAOps(Float64).force_not_nil!(e.values)
      else
        raise InvalidColumnOperationException.new
      end.max
    end

    # Calculates the arithmetic mean of the column values.
    # remove_na If `true` missing values will be excluded from the operation
    # raises `MissingValueException` if `remove_na` is `false` but the data contains missing values
    # raises `InvalidColumnOperationException` If the type of the column is not numeric
    def mean(remove_na = false)
      case e = self
      when Int32Col
        remove_na ? NAOps(Int32).filter_not_nil(e.values) : NAOps(Int32).force_not_nil!(e.values)
      when Int64Col
        remove_na ? NAOps(Int64).filter_not_nil(e.values) : NAOps(Int64).force_not_nil!(e.values)
      when Float64Col
        remove_na ? NAOps(Float64).filter_not_nil(e.values) : NAOps(Float64).force_not_nil!(e.values)
      else
        raise InvalidColumnOperationException.new
      end.mean
    end

    # Calculates the sum of the column values.
    # remove_na If `true` missing values will be excluded from the operation
    # raises `MissingValueException` if `remove_na` is `false` but the data contains missing values
    # raises `InvalidColumnOperationException` If the type of the column is not numeric
    def sum(remove_na = false)
      case e = self
      when Int32Col
        remove_na ? NAOps(Int32).filter_not_nil(e.values) : NAOps(Int32).force_not_nil!(e.values)
      when Int64Col
        remove_na ? NAOps(Int64).filter_not_nil(e.values) : NAOps(Int64).force_not_nil!(e.values)
      when Float64Col
        remove_na ? NAOps(Float64).filter_not_nil(e.values) : NAOps(Float64).force_not_nil!(e.values)
      else
        raise InvalidColumnOperationException.new
      end.sum
    end

    # Calculates the median of the column values.
    # remove_na If `true` missing values will be excluded from the operation
    # raises `MissingValueException` if `remove_na` is `false` but the data contains missing values
    # raises `InvalidColumnOperationException` If the type of the column is not numeric
    def median(remove_na = false)
      case e = self
      when Int32Col
        remove_na ? NAOps(Int32).filter_not_nil(e.values) : NAOps(Int32).force_not_nil!(e.values)
      when Int64Col
        remove_na ? NAOps(Int64).filter_not_nil(e.values) : NAOps(Int64).force_not_nil!(e.values)
      when Float64Col
        remove_na ? NAOps(Float64).filter_not_nil(e.values) : NAOps(Float64).force_not_nil!(e.values)
      else
        raise InvalidColumnOperationException.new
      end.median
    end

    # Calculates the standard deviation of the column values.
    # remove_na If `true` missing values will be excluded from the operation
    # raises `MissingValueException` if `remove_na` is `false` but the data contains missing values
    # raises `InvalidColumnOperationException` If the type of the column is not numeric
    def sd(remove_na = false)
      case e = self
      when Int32Col
        remove_na ? NAOps(Int32).filter_not_nil(e.values) : NAOps(Int32).force_not_nil!(e.values)
      when Int64Col
        remove_na ? NAOps(Int64).filter_not_nil(e.values) : NAOps(Int64).force_not_nil!(e.values)
      when Float64Col
        remove_na ? NAOps(Float64).filter_not_nil(e.values) : NAOps(Float64).force_not_nil!(e.values)
      else
        raise InvalidColumnOperationException.new
      end.sd
    end

    # Calculates the cumulative sum of the column values.
    # An NA value in x causes the corresponding and following elements of the return value to be NA.
    # raises `InvalidColumnOperationException` If the type of the column is not numeric
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

    # Calculates the percentage change between the current and a prior column value.
    # raises `InvalidColumnOperationException` If the type of the receiver column is not numeric
    def pct_change : DataCol
      self / lag(1) + (-1)
    end

    # Returns the "next" column values. Useful for comparing values ahead of the current values.
    # n - positive integer, giving the number of positions to lead by (defaults to 1)
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

    # Returns the "previous" column values. Useful for comparing values ahead of the current values.
    # n - positive integer, giving the number of positions to lead by (defaults to 1)
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

    # Match a text column in a NA-aware manner to create a predicate vector for filtering.
    def matching(missing_as = false, &block) : Array(Bool)
      map { |e| yield e.to_s }.map { |e| e.nil? ? missing_as : e.not_nil! }
    end

    # Returns values as Array of String?
    def as_s
      case self
      when Int32Col, Int64Col, Float64Col, BoolCol, AnyCol
        Array(String?).new(values.size) { |i| values[i].to_s }
      else
        Cast(StringCol).cast(self).values
      end
    end

    # Returns `BoolCol` values as an array of `Bool?`
    def as_b
      Cast(BoolCol).cast(self).values
    end

    # Returns `Int32Col` values as an array of `Int32?`
    def as_i
      Cast(Int32Col).cast(self).values
    end

    # Returns `Int32Col or Int64Col` values as an array of `Int64?`
    def as_i64
      case self
      when Int32Col then Array(Int64?).new(values.size) { |i| self[i].try &.to_i64 }
      else
        Cast(Int64Col).cast(self).values
      end
    end

    # Returns `Int32Col, Int64Col, Float64Col` values as an array of `Float64?`
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
        raise CrysdaException.new ("Could not cast column '#{col.name}' of type '#{col.class.name}' to type '#{R}'") if v.nil?
        v
      end
    end
  end

  # Column for String types
  struct StringCol < DataCol
    def initialize(@name : String, val : Array(String?))
      super(@name)
      @values = Array(String?).new(val.size) { |i| val[i] }
    end

    def values : Array(String?)
      @values
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a = @values[left]
      b = @values[right]
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
      @values.map_non_nil {|v| v.not_nil! {{op.id}} val}.nil_as_false
    end

    def {{op.id}}(val : StringCol)
      @values.zip(val.values).map {|a,b| a && b ? a {{op.id}} b : nil}.nil_as_false
    end
    {% end %}

    def plus(val : DataCol)
      raise UnSupportedOperationException.new unless val.is_a?(DataCol)
      StringCol.new(Crysda.temp_colname,
        @values.map_with_index { |v, i| na_aware_plus(v, val.values[i].to_s) })
    end

    protected def na_aware_plus(first : String?, second : String?)
      (first.nil? || second.nil?) ? nil : first.not_nil! + second.not_nil!
    end
  end

  # Column for Float64 types
  struct Float64Col < DataCol
    private alias Op = Proc(Float64, Float64, Float64)

    def initialize(@name : String, val : Array(Float64?))
      super(@name)
      @values = Array(Float64?).new(val.size) { |i| val[i] }
    end

    def values : Array(Float64?)
      @values
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a = @values[left]
      b = @values[right]
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
      values.map_non_nil {|v| v.not_nil! {{op.id}} val}.nil_as_false
    end

    def {{op.id}}(val : Float64Col)
    values.zip(val.values).map {|a,b| a && b ? a {{op.id}} b : nil}.nil_as_false
  end
    {% end %}

    def plus(val)
      do_op(val,
        ->(a : Float64, b : Float64) { a + b })
    end

    def minus(val)
      do_op(val,
        ->(a : Float64, b : Float64) { a - b })
    end

    def div(val)
      do_op(val,
        ->(a : Float64, b : Float64) { a / b })
    end

    def times(val)
      do_op(val,
        ->(a : Float64, b : Float64) { a * b })
    end

    private def do_op(val, op : Op)
      v = case val
          when Float64Col
            Array(Float64?).new(values.size) { |i| NAOps(Float64).na_aware_op(@values[i], val.values[i], op) }
          when Int32Col, Int64Col
            Array(Float64?).new(values.size) { |i| NAOps(Float64).na_aware_op(@values[i], val.values[i].try &.to_f64, op) }
          when Number
            Array(Float64?).new(values.size) { |i| NAOps(Float64).na_aware_op(@values[i], val.to_f64, op) }
          else
            raise UnSupportedOperationException.new
          end
      Utils.handle_union(Crysda.temp_colname, v)
    end
  end

  # Column for Int32 types
  struct Int32Col < DataCol
    private alias Op = Proc(Int32, Int32, Int32)

    def initialize(@name : String, val : Array(Int32?))
      super(@name)
      @values = Array(Int32?).new(val.size) { |i| val[i] }
    end

    def values : Array(Int32?)
      @values
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a = @values[left]
      b = @values[right]

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
      values.map_non_nil {|v| v.not_nil! {{op.id}} val}.nil_as_false
    end

    def {{op.id}}(val : Int32Col | Int64Col | Float64Col)
    values.zip(val.values).map {|a,b| a && b ? a {{op.id}} b : nil}.nil_as_false
    end
    {% end %}

    def plus(val)
      do_op(val, ->(a : Int32, b : Int32) { a + b },
        ->(a : Float64, b : Float64) { a + b })
    end

    def minus(val)
      do_op(val, ->(a : Int32, b : Int32) { a - b },
        ->(a : Float64, b : Float64) { a - b })
    end

    def div(val)
      double_op(val, ->(a : Float64, b : Float64) { a / b })
    end

    def times(val)
      do_op(val, ->(a : Int32, b : Int32) { a * b },
        ->(a : Float64, b : Float64) { a * b })
    end

    private def do_op(val, int_op : (Int32, Int32) -> Int32, dbl_op : (Float64, Float64) -> Float64)
      case val
      when Int32Col   then handle_op(val, int_op)
      when Int64Col   then handle_op(val, int_op)
      when Float64Col then double_op(val, dbl_op)
      when Int32      then handle_op(val, int_op)
      when Float64    then double_op(val, dbl_op)
      else
        raise CrysdaException.new ("Unsupported + operation for type #{val.class}")
      end
    end

    private def double_op(val, op : (Float64, Float64) -> Float64)
      v = case val
          when Int32Col
            Array(Float64?).new(values.size) { |i| NAOps(Float64).na_aware_op(@values[i].try &.to_f64, val.values[i].try &.to_f64, op) }
          when Float64Col
            Array(Float64?).new(values.size) { |i| NAOps(Float64).na_aware_op(@values[i].try &.to_f64, val.values[i], op) }
          when Float64, Float32
            Array(Float64?).new(values.size) { |i| NAOps(Float64).na_aware_op(@values[i].try &.to_f64, val.to_f, op) }
          end
      Utils.handle_union(Crysda.temp_colname, v)
    end

    private def handle_op(val, op : Op)
      v = case val
          when Int32Col
            Array(Int32?).new(values.size) { |i| NAOps(Int32).na_aware_op(@values[i], val.values[i], op) }
          when Int64Col
            Array(Int32?).new(values.size) { |i| NAOps(Int32).na_aware_op(@values[i], val.values[i].try &.to_i32, op) }
          when Number # Int32
            Array(Int32?).new(values.size) { |i| NAOps(Int32).na_aware_op(@values[i], val, op) }
          else
            raise CrysdaException.new ("Unsupported + operation for type #{typeof(val)}")
          end
      Utils.handle_union(Crysda.temp_colname, v)
    end
  end

  # Column for Int64 types
  struct Int64Col < DataCol
    private alias Op = Proc(Int64, Int64, Int64)

    def initialize(@name : String, val : Array(Int64?))
      super(@name)
      @values = Array(Int64?).new(val.size) { |i| val[i] }
    end

    def values : Array(Int64?)
      @values
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a = @values[left]
      b = @values[right]
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
      values.map_non_nil {|v| v.not_nil! {{op.id}} val}.nil_as_false
    end

    def {{op.id}}(val : Int64Col)
    values.zip(val.values).map {|a,b| a && b ? a {{op.id}} b : nil}.nil_as_false
    end
    {% end %}

    def plus(val)
      do_op(val, ->(a : Int64, b : Int64) { a + b },
        ->(a : Float64, b : Float64) { a + b })
    end

    def minus(val)
      do_op(val, ->(a : Int64, b : Int64) { a - b },
        ->(a : Float64, b : Float64) { a - b })
    end

    def div(val)
      double_op(val, ->(a : Float64, b : Float64) { a / b })
    end

    def times(val)
      do_op(val, ->(a : Int64, b : Int64) { a * b },
        ->(a : Float64, b : Float64) { a * b })
    end

    private def do_op(val, int_op : (Int64, Int64) -> Int64, dbl_op : (Float64, Float64) -> Float64)
      case val
      when Int32Col   then handle_op(val, int_op)
      when Int64Col   then handle_op(val, int_op)
      when Float64Col then double_op(val, dbl_op)
      when Int32      then handle_op(val, int_op)
      when Int64      then handle_op(val, int_op)
      when Float64    then double_op(val, dbl_op)
      else
        raise CrysdaException.new ("Unsupported + operation for type #{typeof(val)}")
      end
    end

    private def double_op(val, op : (Float64, Float64) -> Float64)
      v = case val
          when Float64Col
            Array(Float64?).new(values.size) { |i| NAOps(Float64).na_aware_op(@values[i].try &.to_f64, val.values[i].try &.to_f64, op) }
          when Float64, Float32
            Array(Float64?).new(values.size) { |i| NAOps(Float64).na_aware_op(@values[i].try &.to_f64, val.to_f, op) }
          else
            raise UnSupportedOperationException.new
          end
      Utils.handle_union(Crysda.temp_colname, v)
    end

    private def handle_op(val, op : Op)
      v = case val
          when Int64Col
            Array(Int64?).new(values.size) { |i| NAOps(Int64).na_aware_op(@values[i], val.values[i], op) }
          when Int64
            Array(Int64?).new(values.size) { |i| NAOps(Int64).na_aware_op(@values[i], val, op) }
          when Int32Col
            Array(Int64?).new(values.size) { |i| NAOps(Int64).na_aware_op(@values[i], val.values[i].try &.to_i64, op) }
          when Int32
            Array(Int64?).new(values.size) { |i| NAOps(Int64).na_aware_op(@values[i], val.to_i64, op) }
          else
            raise CrysdaException.new ("Unsupported + operation for type #{typeof(val)}")
          end
      Utils.handle_union(Crysda.temp_colname, v)
    end
  end

  # Column for Bool types
  struct BoolCol < DataCol
    def initialize(@name : String, val : Array(Bool?))
      super(@name)
      @values = Array(Bool?).new(val.size) { |i| val[i] }
    end

    def values : Array(Bool?)
      @values
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a = @values[left]
      b = @values[right]
      case
      when a == b then 0
      when a.nil? then null_last ? 1 : -1
      when b.nil? then null_last ? -1 : 1
      else
        (a != b) ? (a) ? -1 : 1 : 0
      end
    end
  end

  # Column for types other than implemented types and part of `Any` union type.
  struct AnyCol < DataCol
    def initialize(@name : String, val : Array(Any))
      super(@name)
      @values = Array(Any?).new(val.size) { |i| val[i] }
    end

    def values : Array(Any)
      @values
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a = @values[left]
      b = @values[right]
      case
      when a == b then 0
      when a.nil? then null_last ? 1 : -1
      when b.nil? then null_last ? -1 : 1
      else
        (a != b) ? (a) ? -1 : 1 : 0
      end
    end
  end

  # Column for DataFrame types. This is used in nesting the dataframes inside a dataframe
  struct DFCol < DataCol
    def initialize(@name : String, val : Array(DataFrame?))
      super(@name)
      @values = Array(DataFrame?).new(val.size) { |i| val[i] }
    end

    def values : Array(DataFrame?)
      @values
    end

    def compare(left : Int32, right : Int32, null_last = true) : Int32
      a = @values[left]
      b = @values[right]
      case
      when a == b then 0
      when a.nil? then null_last ? 1 : -1
      when b.nil? then null_last ? -1 : 1
      else
        (a != b) ? (a) ? -1 : 1 : 0
      end
    end
  end
end
