require "random"
require "log"

module Crysda
  private struct SimpleDataFrame
    include DataFrame
    getter cols : Array(DataCol)

    def initialize(vals : Array(DataCol))
      @cols = vals
      # validate input columns
      @cols.map(&.name).tap do |v|
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
      @cols.map(&.name)
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
      unless columns.all?(&.in? names)
        raise SelectException.new("not all selected columns (#{columns.to_a.join(",")}) are contained in table")
      end
      raise SelectException.new("Columns must not be selected more than once") unless columns.to_a.uniq.size == columns.size
      columns.reduce(SimpleDataFrame.new) { |df, name| df.add_column(self[name]) }
    end

    def add_column(tf : ColumnFormula) : DataFrame
      mut = tf.expression.call(self.ec)
      col = Utils.any_as_column(mut, tf.name, num_row)

      raise ColumnException.new("New column #{col.name} has inconsistent length #{col.values.size}, against #{num_row}") unless col.values.size == num_row
      raise ColumnException.new("Missing name in new columns") if col.name.starts_with?("temp_col_")

      names.includes?(col.name) ? replace_column(col) : add_column(col)
    end

    def filter(&block : RowPredicate) : DataFrame
      index = block.call(self.ec)
      raise CrysdaException.new("filter index has incompatible length of #{index.size}, rows : #{num_row}") unless index.size == num_row

      # Count matching rows first to pre-allocate
      new_size = index.count(true)
      return SimpleDataFrame.new(@cols.map { |c| empty_col_like(c) }) if new_size == 0

      SimpleDataFrame.new(
        @cols.map do |c|
          filter_column(c, index, new_size)
        end
      )
    end

    # Create empty column of same type
    private def empty_col_like(c : DataCol) : DataCol
      case c
      when Float64Col then Float64Col.new(c.name, [] of Float64?)
      when Int32Col   then Int32Col.new(c.name, [] of Int32?)
      when Int64Col   then Int64Col.new(c.name, [] of Int64?)
      when StringCol  then StringCol.new(c.name, [] of String?)
      when BoolCol    then BoolCol.new(c.name, [] of Bool?)
      when AnyCol     then AnyCol.new(c.name, [] of Any)
      when DFCol      then DFCol.new(c.name, [] of DataFrame?)
      else                 raise UnSupportedOperationException.new
      end
    end

    # Optimized filter that works directly on internal storage
    private def filter_column(c : DataCol, index, new_size : Int32) : DataCol
      case col = c
      when Float64Col
        new_data = Slice(Float64).new(new_size, 0.0)
        new_bitmap = NullBitmap.new(new_size)
        j = 0
        col.raw_data.size.times do |i|
          if index.unsafe_fetch(i)
            new_data[j] = col.raw_data.unsafe_fetch(i)
            new_bitmap.set(j) if col.bitmap[i]
            j += 1
          end
        end
        Float64Col.new(col.name, new_data, new_bitmap)
      when Int32Col
        new_data = Slice(Int32).new(new_size, 0)
        new_bitmap = NullBitmap.new(new_size)
        j = 0
        col.raw_data.size.times do |i|
          if index.unsafe_fetch(i)
            new_data[j] = col.raw_data.unsafe_fetch(i)
            new_bitmap.set(j) if col.bitmap[i]
            j += 1
          end
        end
        Int32Col.new(col.name, new_data, new_bitmap)
      when Int64Col
        new_data = Slice(Int64).new(new_size, 0_i64)
        new_bitmap = NullBitmap.new(new_size)
        j = 0
        col.raw_data.size.times do |i|
          if index.unsafe_fetch(i)
            new_data[j] = col.raw_data.unsafe_fetch(i)
            new_bitmap.set(j) if col.bitmap[i]
            j += 1
          end
        end
        Int64Col.new(col.name, new_data, new_bitmap)
      when StringCol
        new_data = Array(String).new(new_size, "")
        new_bitmap = NullBitmap.new(new_size)
        j = 0
        col.raw_data.size.times do |i|
          if index.unsafe_fetch(i)
            new_data[j] = col.raw_data.unsafe_fetch(i)
            new_bitmap.set(j) if col.bitmap[i]
            j += 1
          end
        end
        StringCol.new(col.name, new_data, new_bitmap)
      when BoolCol
        BoolCol.new(col.name, col.values.select_with_index { |_, i| index[i] })
      when AnyCol
        AnyCol.new(col.name, col.values.select_with_index { |_, i| index[i] })
      when DFCol
        DFCol.new(col.name, col.values.select_with_index { |_, i| index[i] })
      else
        raise UnSupportedOperationException.new
      end
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
      permutation = (0..(num_row - 1)).to_a.sort { |a, b| compare(by, a, b) }

      # apply permutation to all columns using optimized internal access
      SimpleDataFrame.new(cols.map do |v|
        permute_column(v, permutation)
      end)
    end

    # Optimized permutation that works directly on internal storage
    private def permute_column(c : DataCol, perm : Array(Int32)) : DataCol
      case col = c
      when Float64Col
        new_data = Slice(Float64).new(perm.size) { |i| col.raw_data.unsafe_fetch(perm.unsafe_fetch(i)).as(Float64) }
        new_bitmap = NullBitmap.new(perm.size)
        perm.each_with_index { |src, dst| new_bitmap.set(dst) if col.bitmap[src] }
        Float64Col.new(col.name, new_data, new_bitmap)
      when Int32Col
        new_data = Slice(Int32).new(perm.size) { |i| col.raw_data.unsafe_fetch(perm.unsafe_fetch(i)).as(Int32) }
        new_bitmap = NullBitmap.new(perm.size)
        perm.each_with_index { |src, dst| new_bitmap.set(dst) if col.bitmap[src] }
        Int32Col.new(col.name, new_data, new_bitmap)
      when Int64Col
        new_data = Slice(Int64).new(perm.size) { |i| col.raw_data.unsafe_fetch(perm.unsafe_fetch(i)).as(Int64) }
        new_bitmap = NullBitmap.new(perm.size)
        perm.each_with_index { |src, dst| new_bitmap.set(dst) if col.bitmap[src] }
        Int64Col.new(col.name, new_data, new_bitmap)
      when StringCol
        new_data = Array(String).new(perm.size) { |i| col.raw_data.unsafe_fetch(perm.unsafe_fetch(i)) }
        new_bitmap = NullBitmap.new(perm.size)
        perm.each_with_index { |src, dst| new_bitmap.set(dst) if col.bitmap[src] }
        StringCol.new(col.name, new_data, new_bitmap)
      when BoolCol
        BoolCol.new(col.name, Array(Bool?).new(perm.size) { |idx| col.values[perm[idx]] })
      when AnyCol
        AnyCol.new(col.name, Array(Any?).new(perm.size) { |idx| col.values[perm[idx]] })
      when DFCol
        DFCol.new(col.name, Array(DataFrame?).new(perm.size) { |idx| col.values[perm[idx]] })
      else
        raise UnSupportedOperationException.new
      end
    end

    def group_by(by : Iterable(String)) : DataFrame
      Log.warn { "Grouping with empty attribute list is unlikely to have meaningful semantics" } unless by.size > 0

      # todo  test if data is already grouped by the given `by` and skip regrouping if so

      # take all grouping columns
      group_cols = self.select(by)

      raise CrysdaException.new("Could not find all grouping columns") unless group_cols.num_col == by.size

      # Special case: empty by list means all rows in one group with a unique random key
      # This ensures different dataframes get different group keys (important for joins)
      if by.empty?
        random_key = Random.rand(Int64)
        empty_key = GroupKey.new([AnyVal[random_key]])
        l_groups = [DataGroup.new(empty_key, self)]
        return GroupedDataFrame.new(by, l_groups)
      end

      # Optimized row hashing - compute hashes directly from column data
      row_hashes = compute_row_hashes(group_cols)

      # Group by hash value
      group_map = Hash(Int64, Array(Int32)).new { |h, k| h[k] = Array(Int32).new }
      row_hashes.each_with_index { |hash, idx| group_map[hash] << idx }

      # Build group indices with original row values for GroupKey
      group_indices = group_map.map do |hash, indices|
        first_idx = indices.first
        key_vals = group_cols.cols.map { |c| AnyVal[c[first_idx]] }
        GroupIndex.new(GroupKey.new(key_vals), indices)
      end

      l_groups = group_indices.map { |g| DataGroup.new(g.group_hash, extract_group_by_index(g, self)) }

      # preserve column structure in empty data-frames
      l_groups = [DataGroup.new(GroupKey.new([AnyVal[1]]), self)] if l_groups.empty?

      GroupedDataFrame.new(by, l_groups)
    end

    # Optimized row hash computation - avoids AnyVal boxing for numeric columns
    private def compute_row_hashes(group_cols : DataFrame) : Array(Int64)
      cols = group_cols.cols
      n = group_cols.num_row

      # Pre-compute column hashes for each row
      hashes = Array(Int64).new(n, 17_i64)

      cols.each do |col|
        case c = col
        when Int32Col
          n.times do |i|
            h = if c.bitmap[i]
                  HashBuilder::HASH_NULL
                else
                  HashBuilder.hash_i32(c.raw_data.unsafe_fetch(i), hashes.unsafe_fetch(i))
                end
            hashes[i] = h
          end
        when Int64Col
          n.times do |i|
            h = if c.bitmap[i]
                  HashBuilder::HASH_NULL
                else
                  HashBuilder.hash_i64(c.raw_data.unsafe_fetch(i), hashes.unsafe_fetch(i))
                end
            hashes[i] = h
          end
        when Float64Col
          n.times do |i|
            h = if c.bitmap[i]
                  HashBuilder::HASH_NULL
                else
                  HashBuilder.hash_f64(c.raw_data.unsafe_fetch(i), hashes.unsafe_fetch(i))
                end
            hashes[i] = h
          end
        when StringCol
          n.times do |i|
            h = if c.bitmap[i]
                  HashBuilder::HASH_NULL
                else
                  HashBuilder.hash_str(c.raw_data.unsafe_fetch(i), hashes.unsafe_fetch(i))
                end
            hashes[i] = h
          end
        else
          # Fallback for other column types
          hasher = HashBuilder.new
          n.times do |i|
            hashes[i] = hasher.hashcode(AnyVal[col[i]])
            hashes[i] = HashBuilder.combine(hashes.unsafe_fetch(i), hashes[i])
          end
        end
      end

      hashes
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
        new_data = Slice(Float64).new(gid.size) { |i| c.raw_data.unsafe_fetch(gid[i]).as(Float64) }
        new_bitmap = NullBitmap.new(gid.size)
        gid.size.times { |i| new_bitmap.set(i) if c.bitmap[gid[i]] }
        Float64Col.new(c.name, new_data, new_bitmap)
      when Int64Col
        new_data = Slice(Int64).new(gid.size) { |i| c.raw_data.unsafe_fetch(gid[i]).as(Int64) }
        new_bitmap = NullBitmap.new(gid.size)
        gid.size.times { |i| new_bitmap.set(i) if c.bitmap[gid[i]] }
        Int64Col.new(c.name, new_data, new_bitmap)
      when Int32Col
        new_data = Slice(Int32).new(gid.size) { |i| c.raw_data.unsafe_fetch(gid[i]).as(Int32) }
        new_bitmap = NullBitmap.new(gid.size)
        gid.size.times { |i| new_bitmap.set(i) if c.bitmap[gid[i]] }
        Int32Col.new(c.name, new_data, new_bitmap)
      when StringCol
        new_data = Array(String).new(gid.size) { |i| c.raw_data.unsafe_fetch(gid[i]) }
        new_bitmap = NullBitmap.new(gid.size)
        gid.size.times { |i| new_bitmap.set(i) if c.bitmap[gid[i]] }
        StringCol.new(c.name, new_data, new_bitmap)
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
