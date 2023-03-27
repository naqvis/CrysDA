module Crysda
  private struct GroupKey
    @val : Array(AnyVal)
    NA_GROUP_HASH = Int32::MAX - 123

    def initialize(arr : Array(AnyVal))
      @val = arr
      @hasher = HashBuilder.new
    end

    def sort_key # : Int32
      # we make the assumption here that group columns are as in `by`
      res = @val.map { |v| @hasher.hashcode(v) }
      @hasher.hashcode(res)
    end
  end

  private record GroupIndex, group_hash : GroupKey, row_indices : Array(Int32) do
    delegate :[], :[]?, :size, to: @row_indices
  end

  private record DataGroup, group_key : GroupKey, df : DataFrame

  private struct GroupedDataFrame
    include DataFrame
    getter data_groups : Array(DataGroup)
    getter by : Array(String)

    def initialize(@by, @data_groups)
    end

    def num_row : Int32
      @data_groups.map(&.df.num_row).sum
    end

    def num_col : Int32
      @data_groups.first.df.num_col
    end

    def names : Array(String)
      @data_groups.first.df.names
    end

    def cols : Array(DataCol)
      ungroup().cols
    end

    def rows : Iterator(DataFrameRow)
      GDFIterator.new(self)
    end

    def group_offsets : Array(Int32)
      group_sizes = @data_groups.map(&.df.num_row)
      ([0] + group_sizes[...group_sizes.size - 1]).cumsum.map(&.to_i32)
    end

    def row(index : Int32) : DataFrameRow
      grp_offset = group_offsets
      grp_idx = grp_offset.count(&.<= index) - 1
      row_offset = grp_offset.reverse_each.find(&.<= index) || raise CrysdaException.new("Row ##{index} not found")
      @data_groups[grp_idx].df.row(index - row_offset)
    end

    def [](name : String) : DataCol
      ungroup()[name]
    end

    def select(columns : Iterable(String)) : DataFrame
      grps = self.by.dup + (columns - self.by)
      GroupedDataFrame.new(by, @data_groups.map { |e| DataGroup.new(e.group_key, e.df.select(grps)) })
    end

    def filter(&block : RowPredicate) : DataFrame
      bind_rows(@data_groups.map(&.df.filter(&block))).group_by(by)
    end

    def add_column(tf : ColumnFormula) : DataFrame
      bind_rows(@data_groups.map(&.df.add_column(tf))).group_by(by)
    end

    def sort_by(by : Iterable(String)) : DataFrame
      GroupedDataFrame.new(self.by, @data_groups.map { |e| DataGroup.new(e.group_key, e.df.sort_by(by)) })
    end

    def group_by(by : Iterable(String)) : DataFrame
      ungroup().group_by(by)
    end

    def ungroup : DataFrame
      return DataFrame.empty if @data_groups.empty?
      bind_rows(@data_groups.map(&.df))
    end

    def grouped_by : DataFrame
      return DataFrame.empty if by.empty?
      slice(1).ungroup.select(by)
    end

    def groups : Array(DataFrame)
      return Array(DataFrame).new if @data_groups.empty?
      Array(DataFrame).new(@data_groups.size) { |i| @data_groups[i].df }
    end

    def summarize(sum_rules : Array(ColumnFormula)) : DataFrame
      bind_rows(@data_groups.map do |gdf|
        group_tup = gdf.df.select(by).take(1)
        summary = gdf.df.summarize(sum_rules)
        bind_cols(group_tup, summary, rename_duplicates: false)
      end)
    end

    def transform_groups(&trans : (DataFrame) -> DataFrame) : GroupedDataFrame
      GroupedDataFrame.new(by, @data_groups.map { |e| DataGroup.new(e.group_key, trans.call(e.df)) })
    end

    # make sure that by-NA groups come last here (see specs)
    def hash_sorted
      GroupedDataFrame.new(by, @data_groups.sort_by { |v| v.group_key.sort_key })
    end

    private class GDFIterator
      include Iterator(DataFrameRow)

      def initialize(@df : DataFrame)
        @cur_row = 0
      end

      def next
        return stop unless @cur_row < @df.num_row
        ret = @df.row(@cur_row)
        @cur_row += 1
        ret
      end
    end
  end
end
