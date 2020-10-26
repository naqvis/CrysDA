require "regex"

module Crysda
  # `Proc` definition for Column Selector.
  alias ColumnSelector = (ColNames) -> Array(Bool) | Array(Bool?)
  # `NamedTuple` used to `rename` columns. name represents the current **name** of column which should be renamed to **with** value.
  alias RenamePair = NamedTuple(name: String, with: String)

  # Exception raised during `select` operations
  class SelectException < Exception
  end

  # Exception raised when column is not found inside dataset.
  class ColumnNotFoundException < Exception
    def initialize(name : String)
      super("Column '#{name}' not found in data frame")
    end
  end

  # Exception raised when an invalid column selection is being made, or when one tries to use both positive and negative selections.
  class InvalidColumnSelectException < Exception
    def initialize(col_names : Array(String), selection : Array(Bool?))
      collapsed = col_names.zip(selection).to_h.map do |name, selected|
        case selected
        when true  then "+#{name}"
        when false then "-#{name}"
        else
          "<null>"
        end
      end.join(",")
      super("Mixing positive and negative selection does not have meaningful semantics and is not supported:\n#{collapsed}")
    end
  end

  record RenameRule, old_name : String, new_name : String do
    def as_table_formula
      ColumnFormula.new(new_name, TableExpression.new { |df| df[old_name] })
    end
  end

  # Struct which is passed to block and provide helpful methods which can be invoked to perform filtering
  struct ColNames
    getter names : Array(String)

    def initialize(@names)
    end

    def =~(regex : String)
      matches(regex)
    end

    def =~(regex : Regex)
      matches(regex)
    end

    def matches(regex : String)
      matches(Regex.new(regex))
    end

    def matches(regex : Regex)
      names.map { |n| !(n =~ regex).nil? }.false_as_nil
    end

    def starts_with?(prefix : String)
      names.map { |n| n.starts_with?(prefix) }.false_as_nil
    end

    def ends_with?(prefix : String)
      names.map { |n| n.ends_with?(prefix) }.false_as_nil
    end

    def list_of(*col_names : String)
      names.map { |n| n.in?(col_names) }.false_as_nil
    end

    def list_of(col_names : Array(String))
      names.map { |n| col_names.includes?(n) }.false_as_nil
    end

    def all
      Array(Bool).new(names.size, true)
    end

    def [](from : String, to : String)
      rstart = names.index(from) || raise ColumnNotFoundException.new(from)
      rend = names.index(to) || raise ColumnNotFoundException.new(to)
      range = names[rstart..rend]
      names.map { |n| range.includes?(n) }.false_as_nil
    end

    def [](range : Range(String?, String?))
      begc = range.begin
      if begc.nil?
        start_index = 0
      else
        start_index = names.index(begc) || raise ColumnNotFoundException.new(begc)
      end

      endc = range.end
      if endc.nil?
        end_index = names.size - start_index
      else
        end_index = names.index(endc) || raise ColumnNotFoundException.new(endc)
        end_index -= 1 if range.excludes_end?
      end
      cols = names[start_index..end_index]
      names.map { |n| cols.includes?(n) }.false_as_nil
    end

    # normally, there should be no need for them. We just do positive selection and either use
    # `reject` or `select`.
    # BUT: verbs like `gather` still need to support negative selection
    # Performs a negative selection by selecting all columns except the listed ones.
    def except(*cols : String)
      except(cols.to_a)
    end

    def except(cols : Array(String))
      if cols.size == 0
        names.map { |_| true }
      else
        names.map { |n| !cols.includes?(n) }.true_as_nil
      end
    end

    def except(&selector : ColumnSelector)
      selector.call(self).not
    end
  end
end
