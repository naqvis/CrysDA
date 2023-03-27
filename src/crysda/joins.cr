module Crysda
  module DataFrame
    private enum JoinType
      LEFT
      RIGHT
      INNER
      OUTER
    end

    def left_join(right : DataFrame, by : String, suffices : Tuple(String, String) = {".x", ".y"}) : DataFrame
      join(self, right, [by], suffices, type: JoinType::LEFT)
    end

    def left_join(right : DataFrame, by : Iterable(String) = default_by(self, right), suffices : Tuple(String, String) = {".x", ".y"}) : DataFrame
      join(self, right, by, suffices, type: JoinType::LEFT)
    end

    def right_join(right : DataFrame, by : String, suffices : Tuple(String, String) = {".x", ".y"}) : DataFrame
      join(self, right, [by], suffices, type: JoinType::RIGHT)
    end

    def right_join(right : DataFrame, by : Iterable(String) = default_by(self, right), suffices : Tuple(String, String) = {".x", ".y"}) : DataFrame
      join(self, right, by, suffices, type: JoinType::RIGHT)
    end

    def inner_join(right : DataFrame, by : String, suffices : Tuple(String, String) = {".x", ".y"}) : DataFrame
      join(self, right, [by], suffices, type: JoinType::INNER)
    end

    def inner_join(right : DataFrame, by : Iterable(String) = default_by(self, right), suffices : Tuple(String, String) = {".x", ".y"}) : DataFrame
      join(self, right, by, suffices, type: JoinType::INNER)
    end

    def inner_join(right : DataFrame, by : Iterable(Tuple(String, String)), suffices : Tuple(String, String) = {".x", ".y"}) : DataFrame
      join(self, resolve_unequal_by(right, by), by.to_h.keys, suffices, type: :inner)
    end

    # Special case of inner join against distinct right side
    def semi_join(right : DataFrame, by : String) : DataFrame
      semi_join(right, [by])
    end

    def semi_join(right : DataFrame, by : Iterable(Tuple(String, String))) : DataFrame
      semi_join(resolve_unequal_by(right, by), by.to_h.keys)
    end

    def semi_join(right : DataFrame, by : Iterable(String) = default_by(self, right), suffices : Tuple(String, String) = {".x", ".y"}) : DataFrame
      right_reduced = right
        # just keep one instance per group
        .distinct(by)
        # remove non-grouping columns to prevent columns suffixing
        .select(by)

      join(self, right_reduced, by, suffices, type: JoinType::INNER)
    end

    def outer_join(right : DataFrame, by : String, suffices : Tuple(String, String) = {".x", ".y"}) : DataFrame
      join(self, right, [by], suffices, type: JoinType::OUTER)
    end

    def outer_join(right : DataFrame, by : Iterable(String) = default_by(self, right), suffices : Tuple(String, String) = {".x", ".y"}) : DataFrame
      join(self, right, by, suffices, type: JoinType::OUTER)
    end

    private def join(left : DataFrame, right : DataFrame, by : Iterable(String) = default_by(left, right), suffices : Tuple(String, String) = {".x", ".y"}, *, type : JoinType)
      DataFrame.join(left, right, by, suffices, type: type)
    end

    # :nodoc:
    protected def self.join(left : DataFrame, right : DataFrame, by : Iterable(String), suffices : Tuple(String, String) = {".x", ".y"}, *, type : JoinType) : DataFrame
      grouped_left, grouped_right = prep_for_join(by, left, right, suffices)

      # prepare "overhang null-filler blocks" for cartesian products
      # note: `left` as argument is not enough here because of column shuffling and suffixing
      left_nil = nil_row(grouped_left.data_groups.first?.try &.df || empty)
      right_nil = nil_row(grouped_right.data_groups.first?.try &.df || empty)

      right_it = grouped_right.data_groups.each
      left_it = grouped_left.data_groups.each

      right_group = next_or_nil(right_it)
      by_columns = by.to_a
      group_pairs = Array(Tuple(DataFrame, DataFrame)).new

      left_it.each do |left_group|
        ret = loop do
          break if right_group.nil?

          if left_group.group_key.sort_key < right_group.group_key.sort_key # right is ahead of left
            if type.in? [JoinType::LEFT, JoinType::OUTER]
              group_pairs << {left_group.df, right_nil}
            end
            break true
          elsif left_group.group_key == right_group.group_key # left and right are in sync
            group_pairs << {left_group.df, right_group.df}
            right_group = next_or_nil(right_it)
            break true
          else # left is ahead of right
            if type.in? [JoinType::RIGHT, JoinType::OUTER]
              group_pairs << {left_nil, right_group.df}
            end
            right_group = next_or_nil(right_it)
            next
          end
        end
        next if ret

        # consume unpaired left blocks
        if type.in? [JoinType::LEFT, JoinType::OUTER]
          group_pairs << {left_group.df, right_nil}
        else
          break # no more right blocks -> nothing to do with the remaining left blocks for right and inner joins
        end
      end

      # consume rest of right table iterator
      if type.in? [JoinType::RIGHT, JoinType::OUTER]
        while right_group
          group_pairs << {left_nil, right_group.df}
          right_group = next_or_nil(right_it)
        end
      end

      # todo: this could be multi-threaded but be careful to ensure deterministic order
      header = bind_cols(left_nil, right_nil.reject(by_columns)).take(0)
      group_dfs = group_pairs.map { |l, r| cartesian_product_without_by(l, r, by_columns, !(l == left_nil)) }

      # we need to include the header when binding the results to get the correct shape even if the resulting
      # table has no rows
      bind_rows([header] + group_dfs)
    end

    private def default_by(left, right)
      left.names & right.names
    end

    private def self.next_or_nil(iter)
      val = iter.next
      return nil if val.is_a?(Iterator::Stop)
      val
    end

    # rename second to become compliant with first
    private def resolve_unequal_by(df : DataFrame, by : Iterable(Tuple(String, String)))
      # just do something if the pairs are actually unequal
      by.map { |first, second| {second, first} }.reduce(df) { |d, c| d.rename({name: c[0], with: c[1]}) }
    end

    private def self.prep_for_join(by : Array(String), left : DataFrame, right : DataFrame, suffices : Tuple(String, String))
      # detect common no-by columns and apply optional suffixing
      to_be_suffixed = (left.names & right.names) - by.to_a

      grouped_left = (add_suffix(left, to_be_suffixed, suffix: suffices[0])
        # move join columns to the left
        .try { |v| v.select(by + (v.names - by)) }
        .group_by(by).as(GroupedDataFrame)
        ).hash_sorted

      grouped_right = (add_suffix(right, to_be_suffixed, suffix: suffices[1])
        # move join columns to the left
        .try { |v| v.select(by + (v.names - by)) }
        .group_by(by).as(GroupedDataFrame)
        ).hash_sorted

      {grouped_left, grouped_right}
    end

    # Given a data-frame, this method derives a 1-row table with the same colum types but nil as value for all columns.
    private def self.nil_row(df : DataFrame) : DataFrame
      df.cols.reduce(SimpleDataFrame.empty) do |nil_df, col|
        case col
        when Int32Col   then Int32Col.new(col.name, Array(Int32?).new(1, nil))
        when Int64Col   then Int64Col.new(col.name, Array(Int64?).new(1, nil))
        when Float64Col then Float64Col.new(col.name, Array(Float64?).new(1, nil))
        when StringCol  then StringCol.new(col.name, Array(String?).new(1, nil))
        when BoolCol    then BoolCol.new(col.name, Array(Bool?).new(1, nil))
        else
          AnyCol.new(col.name, Array(Any).new(1, nil))
        end.try { |v| nil_df.add_column(v) }
      end
    end

    private def self.add_suffix(df, cols, prefix = "", suffix = "")
      rename_rules = cols.map { |c| RenameRule.new(c, prefix + c + suffix) }
      df.rename(rename_rules)
    end

    # in a strict sense this is not a cartesian product, but the way we call it (for each tuples of `by`,
    # so the by columns are essentially constant here), it should be
    protected def self.cartesian_product_without_by(left, right, by_columns, remove_right_by) : DataFrame
      # first remove columns that are present in both from right-df
      right_slim = right.reject(by_columns)
      left_slim = left.reject(by_columns)
      remove_right_by ? cartesian_product(left, right_slim) : cartesian_product(left_slim, right)
    end

    protected def self.cartesian_product(left, right)
      left_index_replication = (0..(right.num_row - 1)).flat_map { |_| Array(Int32).new(left.num_row) { |i| i } }
      right_index_replication = (0..(right.num_row - 1)).flat_map { |v| Array(Int32).new(left.num_row, v) }

      # replicate data
      left_cartesian = replicate_by_index(left, left_index_replication)
      right_cartesian = replicate_by_index(right, right_index_replication)

      bind_cols(left_cartesian, right_cartesian)
    end

    protected def self.replicate_by_index(df, rep_index) : DataFrame
      rep_cols = df.cols.map do |v|
        case v
        when Float64Col then Float64Col.new(v.name, Array(Float64?).new(rep_index.size) { |i| v.values[rep_index[i]] })
        when Int32Col   then Int32Col.new(v.name, Array(Int32?).new(rep_index.size) { |i| v.values[rep_index[i]] })
        when Int64Col   then Int64Col.new(v.name, Array(Int64?).new(rep_index.size) { |i| v.values[rep_index[i]] })
        when StringCol  then StringCol.new(v.name, Array(String?).new(rep_index.size) { |i| v.values[rep_index[i]] })
        when BoolCol    then BoolCol.new(v.name, Array(Bool?).new(rep_index.size) { |i| v.values[rep_index[i]] })
        when DFCol      then DFCol.new(v.name, Array(DataFrame?).new(rep_index.size) { |i| v.values[rep_index[i]] })
        else
          raise UnSupportedOperationException.new
        end
      end

      SimpleDataFrame.new(rep_cols)
    end
  end
end
