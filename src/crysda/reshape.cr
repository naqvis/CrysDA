module Crysda
  module DataFrame
    DEF_NEST_COLUMN_NAME = "data"

    # spread a key-value pair across multiple columns.
    #
    # key The bare (unquoted) name of the column whose values will be used as column headings.
    # value The bare (unquoted) name of the column whose values will populate the cells.
    # fill If set, missing values will be replaced with this value -  **NOT IMPLEMENTED**
    # convert If set, attempt to do a type conversion will be run on all new columns. This is useful if the value column
    # was a mix of variables that was coerced to a string.
    def spread(key : String, value : String, fill = nil, convert = false) : DataFrame
      # create new columns
      new_cols = self[key].values.uniq

      # make sure that new column names do not exist already
      if new_cols.is_a?(Array(Any))
        raise CrysdaException.new("spread columns do already exist in data-frame") unless (names & new_cols).empty?
      end

      by_spread_grp = group_by(names - [key, value]).as(GroupedDataFrame)
      spread_grp = by_spread_grp.data_groups.map do |v|
        grp_df = v.df
        raise CrysdaException.new("key value mapping is not unique") unless grp_df.select(key).distinct(key).num_row == grp_df.num_row

        spread_blk = SimpleDataFrame.new(Utils.handle_union(key, new_cols))
          .left_join(grp_df.select(key, value))

        grp_spread = SimpleDataFrame.new(spread_blk.as(SimpleDataFrame).rows.to_a.map do |r|
          if r[value].raw.is_a?(DataFrame)
            DFCol.new(r[key].to_s, [r[value].as_df]).as(DataCol)
          else
            AnyCol.new(r[key].to_s, [r[value].raw.as(Any)]).as(DataCol)
          end
        end)

        bind_cols(grp_df.reject(key, value).distinct, grp_spread)
      end

      spread_with_ghashes = spread_grp.bind_rows

      # coerce types of string field columns
      type_coerced = new_cols.map(&.to_s).reverse
        .reduce(spread_with_ghashes) do |df, spread_col|
          df.add_column(spread_col) { |_| Utils.handle_union(spread_col, df[spread_col].values) }
        end

      if (convert)
        type_coerced = new_cols
          # stringify spread column names
          .map(&.to_s)
          # select for string-type columns
          .select { |s| type_coerced[s].is_a?(StringCol) }
          # attempt conversion
          .reverse
          .reduce(type_coerced) { |df, spread_col| convert_type(df, spread_col) }
      end

      unless fill.nil?
        # TODO: Fill not implemented yet
      end
      type_coerced
    end

    # gather takes multiple columns and collapses into key-value pairs, duplicating all other columns as needed. You use
    # gather() when you notice that you have columns that are not variables.
    #
    # key Name of the key column to create in output.
    # value Name of the value column to create in output.
    # columns The colums to gather. The same selectar syntax as for `krangl::select` is supported here
    # convert If TRUE will automatically run `convertType` on the key column. This is useful if the
    #                column names are actually numeric, integer, or logical.
    def gather(key : String, value : String, columns : Array(String) = self.names, convert : Bool = false) : DataFrame
      raise CrysdaException.new("the column selection to be `gather`ed must not be empty") if columns.empty?

      gather_cols = self.select(columns)

      # 1) convert each gather column into a block

      gather_blk = gather_cols.cols.map do |col|
        SimpleDataFrame.new(
          StringCol.new(key, Array(String?).new(col.size, col.name.as?(String))),
          make_value_col(col, value)
        )
      end.bind_rows.try { |g| convert ? convert_type(g, key) : g } # optionally try to convert key column

      # 2) row-replicate the non-gathered columns
      rest = reject(gather_cols.names)
      index_replication = rest.cols.map do |col|
        Utils.handle_union(col.name, Array(Any | DataFrame).new(gather_blk.num_row) { |i| col[i % col.size] })
      end.try { |v| SimpleDataFrame.new(v) }

      # 3) combine the gather-block with the replicated index-data
      bind_cols(index_replication, gather_blk)
    end

    def gather(key : String, value : String, columns : ColumnSelector, convert : Bool = false) : DataFrame
      gather(key, value, col_select_as_names(columns), convert)
    end

    # Convenience function to paste together multiple columns into one.
    #
    # colName - Name of the column to add
    # which - Names of columns which should be concatenated together
    # sep - Separator to use between values.
    # remove - If true, remove input columns from output data frame.
    #
    # see `separate`
    def unite(col_name : String, which : Array(String), sep : String = "_", remove : Bool = true) : DataFrame
      raise CrysdaException.new("the column selection to be `unite`ed must not be empty") if which.empty?

      unite_blk = self.select(which)
      unite_res = unite_blk.rows.to_a.map { |r| r.values.map(&.to_s).join(sep) }

      rest = remove ? reject(unite_blk.names) : self

      rest.add_column(col_name) { |_| unite_res }
    end

    def unite(col_name : String, *which : ColumnSelector, sep : String = "_", remove : Bool = true) : DataFrame
      unite(col_name, which: col_select_as_names(reduce_col_selectors(which.to_a)), sep: sep, remove: remove)
    end

    # Given either regular expression or a vector of character positions, separate() turns a single character column into multiple columns.
    #
    # column - Bare column name.
    # into - Names of new variables to create as character vector.
    # sep - Separator between columns. If String, is interpreted as a regular expression. The default value is a regular expression that matches any sequence of non-alphanumeric values.
    # remove - If true, remove input column from output data frame.
    # convert - If set, attempt to do a type conversion will be run on all new columns. This is useful if the value column was a mix of variables that was coerced to a string.
    def separate(column : String, into : Array(String), sep : Regex | String = /[^\w]/, remove : Bool = true, convert : Bool = false) : DataFrame
      sep_col = self[column]
      str_sep = sep.is_a?(String) ? sep.to_regex : sep
      # split column by given delimiter and keep NAs
      split_data = sep_col.as_s.map { |v| v.try &.split(str_sep) || [] of String }
      split_widths = split_data.map { |d| d.try &.size }.reject(&.nil?).uniq
      num_splits = split_widths.first || 0

      raise CrysdaException.new("unequal splits are not yet supported") unless split_widths.size == 1
      raise CrysdaException.new("mismatch between number of splits #{num_splits} and provided new column names '#{into}'") unless num_splits == into.size

      # vertically split into columns and perform optional type conversion
      split_cols = (0..(num_splits - 1)).map { |index| StringCol.new(into[index], split_data.map { |v| v[index] == MISSING_VALUE ? nil : v[index] }) }
        .map { |v|
          # optionally do type conversion
          convert ? Utils.get_col(v) : v
        }

      # column bind rest and separated columns into final result
      rest = remove ? reject(column) : self

      bind_cols(rest, SimpleDataFrame.new(split_cols))
    end

    # Nest repeated values in a list-variable.
    #
    # There are many possible ways one could choose to nest colSelect inside a data frame. nest() creates a list of data
    # frames containing all the nested variables: this seems to be the most useful form in practice.
    #
    # Usage
    #
    # ```
    # nest(data, ..., column_name = "data")
    # ```
    #
    # col_select - A selection of col_select. If not provided, all except the grouping variables are selected.
    # column_name - The name of the new column, as a string or symbol.
    # also see https://github.com/tidyverse/tidyr/blob/master/R/nest.R
    def nest(col_select : ColumnSelector = ColumnSelector.new { |c| c.except(grouped_by().names) },
             column_name : String = DEF_NEST_COLUMN_NAME) : DataFrame
      nest_cols = col_select_as_names(col_select)

      case self
      when GroupedDataFrame
        raise CrysdaException.new("can not nest grouping columns") unless (nest_cols & self.by).empty?

        list_col = groups.map { |g| g.select { |c| c.list_of(nest_cols) } }
        df_cols = Array(DataFrame).new(list_col.size) { |i| list_col[i] }
        grouped_by.add_column(column_name) { |_| df_cols }.ungroup
      when nest_cols.size == names.size # are all columns nested away
        Crysda.dataframe_of(column_name).values(self)
      else
        group_by { |c| c.except(nest_cols) }.nest(col_select)
      end
    end

    # If you have a list-column, this makes each element of the list its own row. It unfolds data vertically. unnest() can handle list-columns that can atomic vectors, lists, or data frames (but not a mixture of the different types).
    def unnest(column_name : String) : DataFrame
      first_or_nil = self[column_name].values.reject(&.nil?).first

      # if the list column is a list we repackage into a single column data-frame with the same name
      if (col = first_or_nil) && !col.is_a?(DataFrame)
        repackaged = case col
                     when Iterable
                       add_column(column_name) { |c| c[column_name].map { |vals| Crysda.dataframe_of(column_name).values(vals).as(DataFrame) } }
                     else
                       raise CrysdaException.new("Unnesting failed because of unsupported list column type")
                     end
        return repackaged.unnest(column_name)
      end

      data_col = self[column_name].values.as(Array(DataFrame?))

      replication_index = data_col.map_with_index { |df, row| Array(Int32).new(df.try &.num_row || 1) { |_| row } }
        .flatten

      left = DataFrame.replicate_by_index(reject(column_name), replication_index)

      unnested = data_col.map { |c| c || DataFrame.empty }.bind_rows

      bind_cols(left, unnested)
    end

    # Turns implicit missing values into explicit missing values. This is a wrapper around `#expand`
    def complete(*column_names : String) : DataFrame
      expand(*column_names).left_join(self, by: column_names.to_a)
    end

    # `expand` is often useful in conjunction with left_join if you want to convert implicit missing values to explicit
    # missing values.
    def expand(*column_names : String) : DataFrame
      dummy_col = Crysda.temp_colname
      folded = column_names
        .map { |c| self.select(c).distinct }
        .reduce(Crysda.dataframe_of(dummy_col).values(1)) { |acc, nxt|
          DataFrame.cartesian_product_without_by(acc, nxt, [dummy_col], true)
        }
      folded.reject(dummy_col).sort_by(*column_names)
    end

    private def make_value_col(col : DataCol, name : String)
      case col
      when Int32Col
        Int32Col.new(name, col.values.dup.as(Array(Int32?)))
      when Int64Col
        Int64Col.new(name, col.values.dup.as(Array(Int64?)))
      when Float64Col
        Float64Col.new(name, col.values.dup.as(Array(Float64?)))
      when StringCol
        StringCol.new(name, col.values.dup.as(Array(String?)))
      when BoolCol
        BoolCol.new(name, col.values.dup.as(Array(Bool?)))
      else
        AnyCol.new(name, col.values.dup.as(Array(Any)))
      end
    end

    # Convert a character vector to logical, integer, numeric, complex or factor as appropriate
    private def convert_type(df : DataFrame, sp_col_name : String) : DataFrame
      sp_col = df[sp_col_name]
      conv_col = Utils.get_col(sp_col)
      df.add_column(sp_col_name) { |_| conv_col }
    end
  end
end
