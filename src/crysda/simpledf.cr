require "random"
require "log"

module Crysda
  private struct SimpleDataFrame
    include DataFrame
    getter cols : Array(DataCol)

    def initialize(vals : Array(DataCol))
      @cols = vals
      # validate input columns
      @cols.map { |c| c.name }.tap do |v|
        raise DuplicateColumnNameException.new(v) unless v.to_set.size == v.size
      end
    end

    def self.empty
      new([] of DataCol)
    end

    def self.new(*cols : DataCol)
      new(cols.to_a)
    end

    def self.new
      new([] of DataCol)
    end

    def num_row : Int32
      first = @cols.first?
      return 0 unless first
      raise UnSupportedOperationException.new unless first.is_a?(DataCol)
      first.values.size
    end

    def num_col : Int32
      @cols.size
    end

    def names : Array(String)
      @cols.map { |v| v.name }
    end

    def [](name : String) : DataCol
      res = @cols.select { |c| c.name == name }
      raise CrysdaException.new("Could not find column '#{name}' in dataframe") unless res.size > 0
      res.first
    end

    def row(index : Int32) : DataFrameRow
      @cols.map do |c|
        raise UnSupportedOperationException.new unless c.is_a?(DataCol)
        raise CrysdaException.new("Row not found in dataframe") unless index < c.values.size
        {c.name, AnyVal[c.values[index]]}
      end.to_h
    end

    def rows : Iterator(DataFrameRow)
      DSFIterator.new(row_data.each, names)
    end

    def select(columns : Iterable(String)) : DataFrame
      unless columns.all? { |e| e.in? names }
        raise SelectException.new("not all selected columns (#{columns.to_a.join(",")}) are contained in table")
      end
      raise SelectException.new("Columns must not be selected more than once") unless columns.to_a.uniq.size == columns.size
      columns.reduce(SimpleDataFrame.new) { |df, name| df.add_column(self[name]) }
    end

    def add_column(tf : ColumnFormula) : DataFrame
      mut = tf.expression.call(self.ec)
      col = Utils.any_as_column(mut, tf.name, num_row)

      raise ColumnException.new("New column #{col.name} has inconsistent length #{col.values.size }, against #{num_row}") unless col.values.size == num_row
      raise ColumnException.new("Missing name in new columns") if col.name.starts_with?("temp_col_")

      names.includes?(col.name) ? replace_column(col) : add_column(col)
    end

    def filter(&block : RowPredicate) : DataFrame
      index = block.call(self.ec)
      raise CrysdaException.new("filter index has incompatible length of #{index.size}, rows : #{num_row}") unless index.size == num_row
      SimpleDataFrame.new(
        @cols.map do |c|
          case c
          when Float64Col
            Float64Col.new(c.name, c.values.select_with_index { |_, i| index[i] })
          when Int32Col
            Int32Col.new(c.name, c.values.select_with_index { |_, i| index[i] })
          when Int64Col
            Int64Col.new(c.name, c.values.select_with_index { |_, i| index[i] })
          when StringCol
            StringCol.new(c.name, c.values.select_with_index { |_, i| index[i] })
          when BoolCol
            BoolCol.new(c.name, c.values.select_with_index { |_, i| index[i] })
          when AnyCol
            AnyCol.new(c.name, c.values.select_with_index { |_, i| index[i] })
          when DFCol
            DFCol.new(c.name, c.values.select_with_index { |_, i| index[i] })
          else
            raise UnSupportedOperationException.new
          end
        end
      )
    end

    def summarize(sum_rules : Array(ColumnFormula)) : DataFrame
      sum_cols = Array(DataCol).new
      sum_rules.each do |cf|
        sum_val = cf.expression.call(self.ec)
        case col = sum_val
        when Int32   then Int32Col.new(cf.name, Array(Int32?).new(1, col))
        when Int64   then Int64Col.new(cf.name, Array(Int64?).new(1, col))
        when Float64 then Float64Col.new(cf.name, Array(Float64?).new(1, col))
        when Bool    then BoolCol.new(cf.name, Array(Bool?).new(1, col))
        when String  then StringCol.new(cf.name, Array(String?).new(1, col))
          # prevent non-scalar summaries.
        when DataCol then raise NonScalarValueException.new(cf, col)
        when Array   then raise NonScalarValueException.new(cf, "Array")
        else
          AnyCol.new(cf.name, [col])
        end.tap { |v| sum_cols << v unless v.nil? }
      end
      SimpleDataFrame.new(sum_cols)
    end

    private def replace_column(col : DataCol)
      index = names.index(col.name) || raise ColumnException.new("column #{col} does not exist in data-frame")
      ncols = cols.dup
      ncols[index] = col
      SimpleDataFrame.new(ncols)
    end

    protected def add_column(col : DataCol)
      # make sure that table is either empty or row number matches table row count
      unless num_row == 0 || col.size == num_row
        raise ColumnException.new("Column lengths of dataframe (#{num_row}) and new column (#{col.size}) differs")
      end

      if names.includes?(col.name)
        raise ColumnException.new("Column '#{col.name}' already exists in dataframe")
      end
      if col.name.starts_with?("temp_col_")
        raise ColumnException.new("Internal temporary column name should not be exposed to user")
      end
      SimpleDataFrame.new(cols + [col])
    end

    def sort_by(by : Iterable(String)) : DataFrame
      # comp_chain = by.to_a.map{|v| self[v].comparator}.reduce{|a,b| a.try &.then(b)}
      # comp_chain = by.map { |v| self[v].comparator }.reduce { |a, b| a.then(b) }

      # comparators = by.map { |v| self[v].comparator }
      # comp_chain = comparators.shift
      # comparators.reduce(comp_chain){ |a, b| a.then(b) }

      # permutation = (0..(num_row - 1)).to_a.sort { |a, b| comp_chain.compare(a, b) }
      permutation = (0..(num_row - 1)).to_a.sort { |a, b| compare(by, a, b) }

      # apply permutation to all columns
      SimpleDataFrame.new(cols.map do |v|
        case v
        when Int32Col   then Int32Col.new(v.name, Array(Int32?).new(num_row) { |idx| v.values[permutation[idx]] })
        when Int64Col   then Int64Col.new(v.name, Array(Int64?).new(num_row) { |idx| v.values[permutation[idx]] })
        when Float64Col then Float64Col.new(v.name, Array(Float64?).new(num_row) { |idx| v.values[permutation[idx]] })
        when BoolCol    then BoolCol.new(v.name, Array(Bool?).new(num_row) { |idx| v.values[permutation[idx]] })
        when StringCol  then StringCol.new(v.name, Array(String?).new(num_row) { |idx| v.values[permutation[idx]] })
        when AnyCol     then AnyCol.new(v.name, Array(Any?).new(num_row) { |idx| v.values[permutation[idx]] })
        else
          raise UnSupportedOperationException.new
        end
      end)
    end

    def group_by(by : Iterable(String)) : DataFrame
      Log.warn { "Grouping with empty attribute list is unlikely to have meaningful semantics" } unless by.size > 0

      # todo  test if data is already grouped by the given `by` and skip regrouping if so

      # take all grouping columns
      group_cols = self.select(by)

      raise CrysdaException.new("Could not find all grouping columns") unless group_cols.num_col == by.size

      empty_by_hash = Random.rand(Int32)

      row_hashes = if by.empty?
                     Array(Int32).new(num_row, empty_by_hash).map { |e| [AnyVal[e]] }
                   else
                     group_cols.row_data.each
                   end

      group_indices = row_hashes.map_with_index { |group, idx| {group, idx} }
        .group_by { |v| v[0] }
        .map do |k, v|
          group_row_indices = v.map(&.[1])
          GroupIndex.new(GroupKey.new(k), group_row_indices)
        end

      l_groups = group_indices.map { |g| DataGroup.new(g.group_hash, extract_group_by_index(g, self)) }

      # preserve column structure in empty data-frames
      l_groups = [DataGroup.new(GroupKey.new([AnyVal[1]]), self)] if l_groups.empty?

      GroupedDataFrame.new(by, l_groups)
    end

    def ungroup : DataFrame
      self
    end

    def grouped_by : DataFrame
      self.class.empty
    end

    def groups : Array(DataFrame)
      [self] of DataFrame
    end

    private def compare(by, a, b) : Int32
      idx = 0
      while idx < by.size
        col = self[by[idx]]
        ret = col.compare(a, b)
        return ret if (ret != 0) || idx == by.size - 1
        idx += 1
      end
      raise CrysdaException.new("compare didn't return any result")
    end

    private def extract_group(col : DataCol, gid : GroupIndex) : DataCol
      case c = col
      when Float64Col
        Float64Col.new(c.name, Array(Float64?).new(gid.size) { |i| c[gid[i]] })
      when Int64Col
        Int64Col.new(c.name, Array(Int64?).new(gid.size) { |i| c[gid[i]] })
      when Int32Col
        Int32Col.new(c.name, Array(Int32?).new(gid.size) { |i| c[gid[i]] })
      when StringCol
        StringCol.new(c.name, Array(String?).new(gid.size) { |i| c[gid[i]] })
      when BoolCol
        BoolCol.new(c.name, Array(Bool?).new(gid.size) { |i| c[gid[i]] })
      when AnyCol
        AnyCol.new(c.name, Array(Any).new(gid.size) { |i| c[gid[i]] })
      else
        raise UnSupportedOperationException.new
      end
    end

    private def extract_group_by_index(gid : GroupIndex, df : SimpleDataFrame) : SimpleDataFrame
      grp_sub_cols = df.cols.map { |c| extract_group(c, gid) }
      SimpleDataFrame.new(grp_sub_cols)
    end

    private class DSFIter
      include Iterable(DataFrameRow)

      def initialize(@iter : Iterable(Array(AnyVal)), @names : Array(String))
      end

      def each
        DSFIterator.new(@iter.each, @names)
      end

      def each_with_index
        DSFIterator.new(@iter.each_with_index, @names)
      end
    end

    class DSFIterator
      include Iterator(DataFrameRow)

      def initialize(@iter : Iterator(Array(AnyVal)), @names : Array(String))
      end

      def next
        val = @iter.next
        return stop if val.is_a?(Iterator::Stop)
        @names.zip(val).to_h
      end
    end

    def_hash @cols

    def ==(other : self)
      val = @cols.size == other.cols.size && @cols == other.cols
      return val unless val
      @cols.each_with_index do |c, i|
        val = false unless c.equals other.cols[i]
        break unless val
      end
      val
    end
  end
end
