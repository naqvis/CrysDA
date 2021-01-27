require "uuid"
require "json"

module Crysda
  # Row predicate `Proc` used in `filter` block.
  alias RowPredicate = ExpressionContext -> Array(Bool) | Array(Bool?)
  alias TableExpression = ExpressionContext -> Any | DataCol | Array(String) | Array(String?) | Array(Int32) | Array(Int32?) |
                                               Array(Int64) | Array(Int64?) | Array(Float32) | Array(Float32?) |
                                               Array(Float64) | Array(Float64?) | Array(Bool) | Array(Bool?) | Array(CustomColumnValue) |
                                               Array(DataFrame) | Array(DataFrame?)

  alias SortExpression = SortingContext -> Any | DataCol
  alias DataFrameRow = ::Hash(String, AnyVal)
  alias SummarizeFunc = SummarizeBuilder -> Nil

  # Base class for storing custom values inside columns. Crystal restricts usage of base `Reference` or `Object` classes in Unions
  # so we need a mechanism to bypass that restriction. Child classes inheriting this base class need to only override `hashcode` method
  # to ensure that call to this method returns the unique and consistent value on each invocation. This `hashcode` is different from one
  # provided by the language, as that ensures the consistency during same run, but provides different value on app different runs.
  # Hashing value is used in Grouping to ensure the order, so relying on Crystal implementation would reveal different grouping results on
  # different runs.
  # Shard provides `HashBuilder` interface which should be used to calculate the `hashcode`, as this ensures the same hashcode returns on
  # each and every invocation. It is advised to override `to_s` methods, so that you see useful information when data is printed to console
  # via `schema` and/or `print` method.
  # **Sample Usage**
  # ```
  # class Address < CustomColumnValue
  #   getter street : String
  #   getter city : String
  #
  #   def initialize(@street, @city)
  #   end
  #
  #   def to_s
  #     "#{street}, #{city}"
  #   end
  #
  #   def to_s(io : IO) : Nil
  #     io << to_s
  #   end
  #
  #   def hashcode : Int64
  #     hb = HashBuilder.new
  #     hb.add(@street).add(@city).hashcode
  #   end
  # end
  # ```
  abstract class CustomColumnValue
    # returns the hash code value for this object.
    # this should be consistent in returning the value
    abstract def hashcode : Int64
  end

  alias Any = String | Int32 | Int64 | Float32 | Float64 | Bool | UUID | Time | Nil | CustomColumnValue | JSON::Any

  class CrysdaException < Exception
  end

  class UnSupportedOperationException < Exception
  end

  class DuplicateColumnNameException < Exception
    def initialize(@names : Array(String))
      dups = @names.group_by { |name| name }.select { |_, v| v.size > 1 }.keys
      msg = case
            when dups.size == 1 then "'#{dups.join}' is already present in data-frame"
            when dups.size > 1  then "'#{dups.join(',')}' are already present in data-frame"
            else
              "This looks like an issue with Crysda. Please submit an issue reproducing the problem/usecase"
            end
      super(msg)
    end
  end

  # Helper for creating a list of Types
  module List(T)
    def self.of(*args : T)
      ArrayList(T).new(*args)
    end

    def self.of
      ArrayList(T).new
    end
  end

  private class ArrayList(T) < CustomColumnValue
    include Iterable(T)
    getter values : Array(T)

    def initialize(@values)
    end

    def self.new
      new([] of T)
    end

    def self.new(*args : T)
      new(args.to_a)
    end

    def each
      @values.each
    end

    def to_s
      "[#{@values.join(", ")}]"
    end

    def to_s(io : IO) : Nil
      io << to_s
    end

    def hashcode : Int64
      hb = HashBuilder.new
      hb.hashcode(@values)
    end

    forward_missing_to @values
  end

  private abstract struct TableContext
    def initialize(@df : DataFrame)
    end

    def [](name : String)
      @df[name]
    end

    def row_num
      (1..@df.num_row).to_a
    end
  end

  # A proxy on the `df` that exposes just parts of the DataFrame api that are relevant for
  # table expressions
  private struct ExpressionContext < TableContext
    getter df : DataFrame

    def initialize(@df)
      super
    end

    def num_row
      @df.num_row
    end

    # A numpy equivalent to
    # `df['color'] = np.where(df['Set']=='Z', 'green', 'red')`
    # See https://stackoverflow.com/questions/19913659/pandas-conditional-creation-of-a-series-dataframe-column
    #
    # In R the corresoponding pattern would be mutate(df, foo=if_else())
    def where(bools : Array(Bool), if_true : Any, if_false : Any) : DataCol
      mut_true = Utils.any_as_column(if_true, Crysda.temp_colname, num_row)
      mut_false = Utils.any_as_column(if_false, Crysda.temp_colname, num_row)

      result = bools.zip(mut_true.values.zip(mut_false.values)).map do |v|
        first, data = v[0], v[1]
        first ? data[0] : data[1]
      end
      Utils.handle_union(Crysda.temp_colname, result)
    end

    def is_na(col_name : String) : Array(Bool)
      self[col_name].values.map(&.nil?)
    end

    def is_not_na(col_name : String) : Array(Bool)
      self[col_name].values.map { |v| !v.nil? }
    end

    def const(something)
      Utils.any_as_column(something, Crysda.temp_colname, num_row)
    end
  end

  # A proxy on the `df` that exposes just parts of the DataFrame api that are relevant for sorting
  private struct SortingContext < TableContext
    getter df : DataFrame

    def initialize(@df)
      super
    end

    # Creates a sorting attribute that inverts the order of argument
    def desc(col : DataCol)
      col.desc
    end

    # Creates a sorting attribute that inverts the order of argument
    def desc(name : String)
      desc(self[name])
    end
  end
end
