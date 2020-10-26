require "./context"

module Crysda
  PRINT_MAX_ROWS    =  10
  PRINT_MAX_WIDTH   = 100
  PRINT_MAX_DIGITS  =   3
  PRINT_ROW_NUMBERS = true

  # A "tabular" data structure representing cases/records (rows), each of which consists of a number of observations or measurements (columns)
  # DataFrame is an immutable object, any mutation will return a new object.
  module DataFrame
    # Number of rows in this dataframe
    abstract def num_row : Int32
    # Number of columns in this dataframe
    abstract def num_col : Int32
    # Ordered list of column names of this data-frame
    abstract def names : Array(String)
    # Ordered list of column in this data-frame
    abstract def cols : Array(DataCol)

    # Returns a column by name
    abstract def [](name : String) : DataCol

    # Returns a row by index
    abstract def row(index : Int32) : DataFrameRow
    # Returns an Iterator over all rows. Per row data is represented as `DataFrameRow`
    abstract def rows : Iterator(DataFrameRow)
    # Create a new data frame with only selected columns
    abstract def select(columns : Iterable(String)) : DataFrame
    # Filter the rows of a table with a single predicate.
    # the filter() function is used to subset a data frame, retaining all rows that satisfy your conditions.
    abstract def filter(&block : RowPredicate) : DataFrame
    # Adds new variables and preserves existing
    abstract def add_column(tf : ColumnFormula) : DataFrame
    # Resorts the receiver in ascending order (small values to go top of table). The first argument defines the
    # primary attribute to sort by. Additional ones are used to resolve ties.
    #
    # Missing values will come last in the sorted table.
    abstract def sort_by(by : Iterable(String)) : DataFrame
    # Creates a grouped data-frame given a list of grouping attributes.
    # Most data operations are done on groups defined by variables. `group_by()` takes the receiver data-frame and
    # converts it into a grouped data-frame where operations are performed "by group". `ungroup()` removes grouping.
    #
    # Most verbs like `add_column()`, `summarize()`, etc. will be executed per group if a grouping is present.
    abstract def group_by(by : Iterable(String)) : DataFrame

    # Removes the grouping (if present from a data frame)
    abstract def ungroup : DataFrame

    # Returns a data-frame of distinct grouping variable tuples for a grouped data-frame.
    # An empty data-frame for ungrouped data
    abstract def grouped_by : DataFrame

    # Returns the groups of a grouped data frame or just a reference to self
    abstract def groups : Array(DataFrame)

    # Creates a summary of a table or a group. The provided expression is expected to evaluate to a scalar value and not into a column.
    # `summarize()` is typically used on grouped data created by group_by(). The output will have one row for each group.
    abstract def summarize(sum_rules : Array(ColumnFormula)) : DataFrame

    # Returns a column by index
    def [](index : Int32)
      self[names[index]]
    end

    # Add a new column and preserve existing ones.
    # ```crystal
    # df.add_column("salary_category") { 3 }             # with constant value
    # df.add_column("age_3y_later") { |e| e["age"] + 3 } # by doing basic column arithmetics
    # ```
    def add_column(col_name : String, &expression : TableExpression) : DataFrame
      add_column(ColumnFormula.new(col_name, expression))
    end

    # add multiple columns
    # ```crystal
    # df.add_columns(
    #   "age_plus3".with { |e| e["age"] + 3 },
    #   "initials".with { |e| e["first_name"].map(&.to_s[0]).concatenate(e["last_name"].map(&.to_s[0])) }
    # )
    # ```
    def add_columns(*cols : ColumnFormula) : DataFrame
      add_columns(cols.to_a)
    end

    def add_columns(cols : Iterable(ColumnFormula)) : DataFrame
      cols.reduce(self) { |df, tf| df.add_column(tf) }
    end

    # Returns a DataFrame containing the new row.
    # The new row length must match the number of columns in the DataFrame
    def add_row(*row)
      raise CrysdaException.new ("Row length must match number of columns") unless row.size == names.size
      new_row = DataFrameRow.new
      names.each_with_index { |n, i| new_row[n] = AnyVal[row[i]] }
      Crysda.dataframe_of(self.rows.to_a + [new_row])
    end

    # Add the row-number as column to data-frame
    def add_row_number(name = "row_number")
      add_column(name) { row_number }.move_left(name)
    end

    # Adds new rows. Missing entries are set to null. The output of bind_rows will contain a column if that column appears in any of the inputs.
    # When row-binding, columns are matched by name, and any missing columns will be filled with NA.
    # Grouping will be discarded when binding rows
    def bind_rows(df : DataFrame) : DataFrame
      bind_rows([df])
    end

    # Add new rows. Missing entries are set to nil. The output of `bind_rows` will contain a column if that column appears in any of the inputs.
    # when row-binding, columns are matched by name, and any missing column will be filled with NA
    # Grouping will be discarded when binding rows
    # ```crystal
    # row1 = {
    #   "person" => "james",
    #   "year"   => 1996,
    #   "weight" => 54.0,
    #   "sex"    => "M",
    # } of String => Any
    #
    # row2 = {
    #   "person" => "nell",
    #   "year"   => 1997,
    #   "weight" => 48.1,
    #   "sex"    => "F",
    # } of String => Any
    # df.bind_rows(row1, row2)
    # ```
    def bind_rows(*rows : Hash(String, Any))
      bind_rows(self, Crysda.dataframe_of(rows.to_a))
    end

    def bind_rows(rows : Iterable(Hash(String, Any)))
      bind_rows(self, Crysda.dataframe_of(rows))
    end

    def bind_rows(*rows : DataFrameRow)
      bind_rows(self, Crysda.dataframe_of(rows.to_a))
    end

    def bind_rows(*df : DataFrame) : DataFrame
      bind_rows(df.to_a)
    end

    def bind_rows(dfs : Iterable(DataFrame)) : DataFrame
      DataFrame.bind_rows(dfs)
    end

    # :nodoc:
    def self.bind_rows(dfs : Iterable(DataFrame)) : DataFrame
      bind_cols = Array(DataCol).new
      col_names = dfs.map(&.names).reduce([] of String) { |acc, right| acc + (right - acc) }
      col_names.each do |cname|
        combined = bind_col_data(dfs, cname)
        bind_cols << Utils.handle_union(cname, combined)
      end

      SimpleDataFrame.new(bind_cols)
    end

    # Add new columns. rows are matched by position, so all data frames must have the same number of rows.
    def bind_cols(cols : Iterable(DataCol))
      SimpleDataFrame.new(cols)
    end

    private def bind_cols(left : DataFrame, right : DataFrame, rename_duplicates = true) : DataFrame
      DataFrame.bind_cols(left, right, rename_duplicates)
    end

    # :nodoc:
    def self.bind_cols(left : DataFrame, right : DataFrame, rename_duplicates = true) : DataFrame
      dup_names = right.names & left.names

      rval = if rename_duplicates && !dup_names.empty?
               name_resolver = DuplicateNameResolver.new(left.names)
               right.rename(dup_names.map { |e| RenameRule.new(e, name_resolver.resolve(e)) })
             else
               right
             end
      SimpleDataFrame.new(left.cols + rval.as(SimpleDataFrame).cols)
    end

    # Counts observations by group.
    #
    # If no grouping attributes are provided the method will respect the grouping of the receiver, or in cases of an
    # ungrouped receiver will simply count the rows in the data.frame
    #
    # selects : The variables to to be used for cross-tabulation.
    # name  : The name of the count column resulting table.
    # ```crystal
    # df.count("column name")
    # ```
    def count(*selects : String, name = "n") : DataFrame
      count(selects.to_a, name)
    end

    def count(selects : Array(String) = [] of String, name = "n") : DataFrame
      case
      when selects.size > 0             then self.select(selects).group_by(selects).summarize(name, TableExpression.new { |ec| ec.df.num_row })
      when self.is_a?(GroupedDataFrame) then self.select(selects).summarize(name, TableExpression.new { |ec| ec.df.num_row })
      else                                   DataFrameBuilder.new(name).values(num_row)
      end
    end

    # Counts expressions
    #
    # If no grouping attributes are provided the method will respect the grouping of the receiver, or in cases of an
    # ungrouped receiver will simply count the rows in the data.frame
    def count_expr(*exprs : TableExpression, name = "n", table_expression : TableExpression? = nil) : DataFrame
      expr_grouped = group_by_expr(*exprs, table_expression: table_expression).tap { |v| print(v) }
      expr_grouped.count(expr_grouped.group_by.names, name: name)
    end

    # Retains only unique/distinct rows
    # selects : Variables to use when determining uniqueness. If there are multiple rows for a given combination of inputs, only the first row will be preserved.
    def distinct(*selects : String) : DataFrame
      distinct(selects.to_a)
    end

    def distinct(selects : Array(String) = self.names) : DataFrame
      group_by(selects).slice(1).ungroup
    end

    # :nodoc:
    # Returns an empty DataFrame with 0 observations
    def self.empty
      SimpleDataFrame.empty
    end

    # AND-filter a table with different filters.
    # Subset rows with filter
    # ```crystal
    # df.filter { |e| e.["age"] == 23 }
    # df.filter { |e| e.["weight"] > 50 }
    # df.filter { |e| e["first_name"].matching { |e| e.starts_with?("Ho") } }
    # ```
    def filter(*predicates : (DataFrame) -> Array(Bool) | Array(Bool?)) : DataFrame
      predicates.reduce(self) { |df, p| df.filter(&p) }
    end

    # filter rows by Row predicate, which is invoked on each row of the dataframe
    # ```crystal
    # df = Crysda.dataframe_of("person", "year", "weight", "sex").values(
    #   "max", 2014, 33.1, "M",
    #   "max", 2016, nil, "M",
    #   "anna", 2015, 39.2, "F",
    #   "anna", 2016, 39.9, "F"
    # )
    # df.filter_by_row { |f| f["year"].as_i > 2015 }.print
    # ```
    def filter_by_row(&row_filter : (DataFrameRow) -> Bool) : DataFrame
      index = Array(Bool).new.tap do |arr|
        rows.each { |v| arr << row_filter.call(v) }
      end
      filter { index }
    end

    # Creates a grouped data-frame given a list of grouping attributes.
    # Most data operations are done on groups defined by variables. `group_by()` takes the receiver data-frame and
    # converts it into a grouped data-frame where operations are performed "by group". `ungroup()` removes grouping.
    #
    # Most verbs like `add_column()`, `summarize()`, etc. will be executed per group if a grouping is present.
    def group_by(*by : String) : DataFrame
      group_by(by.to_a)
    end

    # :nodoc:
    def group_by : DataFrame
      group_by([] of String)
    end

    # Creates a grouped data-frame from a column selector function. See `select()` for details about column selection.
    #
    # Most data operations are done on groups defined by variables. `group_by()` takes the receiver data-frame and
    # converts it into a grouped data-frame where operations are performed "by group". `ungroup()` removes grouping.
    def group_by(&col_selector : ColumnSelector) : DataFrame
      group_by(col_select_as_names(col_selector))
    end

    # Creates a grouped data-frame from one or more table expressions. See `add_column()` for details about table expressions.
    #
    # Most data operations are done on groups defined by variables. `group_by()` takes the receiver data-frame and
    # converts it into a grouped data-frame where operations are performed "by group". `ungroup()` removes grouping.
    def group_by_expr(*exprs : TableExpression, table_expression : TableExpression? = nil) : DataFrame
      group_by_expr(exprs.to_a, table_expression)
    end

    def group_by_expr(table_expression : TableExpression? = nil) : DataFrame
      group_by_expr([] of TableExpression, table_expression)
    end

    def group_by_expr(exprs : Iterable(TableExpression), table_expression : TableExpression? = nil) : DataFrame
      table_exprs = ([table_expression] + exprs).select { |v| !v.nil? }
      col_formula = table_exprs.map_with_index { |func, idx| ColumnFormula.new("group_by_#{idx + 1}", func.not_nil!) }

      add_columns(col_formula).group_by(col_formula.map(&.name))
    end

    # return the top rows from dataframe. default to 5
    def head(rows = 5)
      take(rows)
    end

    # Push some columns to the right end of a data-frame
    def move_right(*col_names : String) : DataFrame
      cols = col_names.to_a
      self.select((names - cols) + cols)
    end

    # Push some columns to the left end of a data-frame
    def move_left(*col_names : String) : DataFrame
      cols = col_names.to_a
      self.select(cols + (names - cols))
    end

    # Prints a dataframe to output (defaults to STDOUT). df.to_s will also work but has no options
    def print(title = "A DataFrame", col_names = true, max_rows = PRINT_MAX_ROWS,
              max_width = PRINT_MAX_WIDTH, max_digits = PRINT_MAX_DIGITS,
              row_numbers = PRINT_ROW_NUMBERS, output = STDOUT)
      output.puts to_string(title, col_names, max_rows, max_width, max_digits, row_numbers)
    end

    # Select or reject columns by predicate
    def reject?(&pred : (DataCol) -> Bool) : DataFrame
      self.select(cols.reject(pred).map(&.name))
    end

    # reject selected columns
    def reject(*columns : String) : DataFrame
      self.reject(columns.to_a)
    end

    def reject(columns : Iterable(String)) : DataFrame
      self.select(names - columns.to_a)
    end

    # reject column by column type
    def reject(col_type : DataCol.class) : DataFrame
      self.select(names - self.select(col_type).names)
    end

    def reject(&col_sel : ColumnSelector) : DataFrame
      self.select { |e| e.except(&col_sel) }
    end

    # remove selected columns
    def reject(*col_sels : ColumnSelector) : DataFrame
      self.select(*col_sels.map { |v| ColumnSelector.new { |x| x.except(&v) } })
    end

    # Rename one or several columns. Positions should be preserved.
    def rename(*cols : RenamePair) : DataFrame
      rename(cols.to_a)
    end

    # Rename one or several columns. Positions should be preserved.
    def rename(cols : Array(RenamePair)) : DataFrame
      rename(cols.map { |c| RenameRule.new(c[:name], c[:with]) })
    end

    def rename(*rules : RenameRule) : DataFrame
      rename rules.to_a
    end

    def rename(rules : Array(RenameRule)) : DataFrame
      # ignore dummy renames like "foo" to "foo" (can happen when doing unequal joins;
      # also because of consistency)
      filtered = rules.select { |r| r.old_name != r.new_name }

      # create column list with new names at old positions
      name_restored_pos = filtered.reduce(names) do |acc, rule|
        acc.map { |v| (v == rule.old_name) ? rule.new_name : v }
      end

      # make sure that renaming rule does not contain duplicates to allow for better error reporting
      renamed = filtered.reduce(self) { |df, rule| df.add_column(rule.as_table_formula).reject(rule.old_name) }

      # restore positions of renamed columns
      renamed.select(name_restored_pos)
    end

    # Returns array of row numbers starting from 1
    def row_number : Array(Int32)
      (1..num_row).to_a
    end

    # Creates a grouped data-frame where each group consists of exactly one line.
    def rowwise : DataFrame
      rows_as_groups = (1..num_row).map { |index|
        DataGroup.new(GroupKey.new([AnyVal[index]]), filter { |_| Array(Bool).new(num_row) { |i| i == index } })
      }

      GroupedDataFrame.new(["_row_"], rows_as_groups)
    end

    # Select random rows from a table.  If receiver is grouped, sampling is done per group.
    # fraction - Fraction of rows to sample
    # replace - Sample with or without replacement
    def sample_frac(fraction : Float64, replace = false) : DataFrame
      self.is_a?(GroupedDataFrame) ? self.transform_groups { |df| df.sample_frac(fraction, replace) } : sample_n((fraction * num_row).round.to_i, replace)
    end

    # Select random rows from a table. If receiver is grouped, sampling is done per group.
    # n - Number of rows to sample
    # replace - Sample with or without replacement
    def sample_n(n : Int32, replace = false) : DataFrame
      return self.transform_groups { |df| df.sample_n(n, replace) } if self.is_a?(GroupedDataFrame)

      raise CrysdaException.new ("can not over-sample data without replace (num_row<#{n})") unless replace || n <= num_row
      raise CrysdaException.new ("Sample size must be greater equal than 0 but was #{n}") unless n >= 0

      rnd = Random.new
      # depending on replacement-mode randomly sample the index vector
      sampling = if replace
                   Array(Int32).new(n) { |_| rnd.rand(num_row) }
                 else
                   shuf_idx = (0..(num_row - 1)).to_a.tap { |arr| arr.shuffle!(rnd) }
                   shuf_idx[...n]
                 end

      SimpleDataFrame.new(cols.map { |col| Utils.handle_union(col, col.name, Array(Any | DataFrame).new(sampling.size) { |i| col[sampling[i]] }) })
    end

    # Prints the schema (that is column names, types, and the first few values per column) of a dataframe to output (defaults to STDOUT).
    def schema(max_digits = 3, max_width = PRINT_MAX_WIDTH, output = STDOUT)
      return self.ungroup.schema(max_digits, max_width) if self.is_a?(GroupedDataFrame)
      topn = self
      output.puts("DataFrame with #{num_row} observations")
      name_padding = topn.cols.map(&.name.size).max? || 0
      type_labels = topn.cols.map { |col| Crysda.get_col_type(col, true) }
      type_padding = type_labels.map(&.size).max? || 0

      val_printer = Utils.create_value_printer(max_digits)

      topn.cols.zip(type_labels).each do |col, lbl|
        sfv = col.values.first(255).map { |v| val_printer.call(v) }.join(", ")
        sfv = sfv.size > max_width ? sfv[...max_width] + "..." : sfv
        output.puts("#{col.name.pad_end(name_padding)} #{lbl.pad_end(type_padding)} #{sfv}")
      end
    end

    # Select or reject columns by predicate
    def select?(&pred : (DataCol) -> Bool) : DataFrame
      self.select(cols.select(pred).map(&.name))
    end

    def select(*columns : String) : DataFrame
      self.select(columns.to_a)
    end

    # Select column by column type
    def select(col_type : DataCol.class) : DataFrame
      self.select(cols.select(col_type).map(&.name))
    end

    # Keeps only the variables that match any of the given expression
    def select(&col_sel : ColumnSelector) : DataFrame
      self.select(col_select_as_names(col_sel))
    end

    def select(*col_sels : ColumnSelector) : DataFrame
      self.select(&reduce_col_selectors(col_sels.to_a))
    end

    def select(which : Array(Bool?)) : DataFrame
      self.select { which }
    end

    # Replace current column names with new ones. The number of provided names must match the number of columns.
    def set_names(*new_name : String) : DataFrame
      set_names(new_name.to_a)
    end

    # Replace current column names with new ones. The number of provided names must match the number of columns.
    def set_names(new_names : Array(String)) : DataFrame
      rename(names.zip(new_names).map { |old, newn| RenamePair.new(name: old, with: newn) })
    end

    # Randomize the row order of a data-frame.
    def shuffle : DataFrame
      sample_n(num_row)
    end

    # Select rows by position while taking into account grouping in a data-frame.
    def slice(*slices : Int32)
      filter &->(ec : ExpressionContext) { ec.df.row_number.map { |e| e.in?(slices) } }
    end

    # Select rows by position while taking into account grouping in a data-frame.
    def slice(slices : Range)
      filter &->(ec : ExpressionContext) { ec.df.row_number.map { |e| slices.includes?(e) } }
    end

    # Resorts the receiver in descending order (small values to go bottom of table). The first argument defines the
    # primary attribute to sort by. Additional ones are used to resolve ties.
    def sort_desc_by(*by : String) : DataFrame
      sort_by(
        by.map do |s|
          SortExpression.new { |e| e.desc(s) }
        end
      )
    end

    def sort_by : DataFrame
      sort_by([] of String)
    end

    def sort_by(*by : String) : DataFrame
      sort_by(by.to_a)
    end

    def sort_by(&exp : SortExpression)
      sort_by([exp])
    end

    def sort_by(exp : Iterable(SortExpression))
      # create derived data frame sort by new columns trash new columns
      sort_bys = exp.map_with_index do |expr, idx|
        ColumnFormula.new("__sort#{idx}", TableExpression.new { |e|
          sc = SortingContext.new(e.df)
          ret = expr.call(sc)
          raise InvalidSortingPredicateException.new(ret) if ret.is_a?(String)
          ret
        })
      end

      sort_by_names = sort_bys.map(&.name)
      add_columns(sort_bys).sort_by(sort_by_names).reject(sort_by_names)
    end

    def summarize(*sum_rules : ColumnFormula) : DataFrame
      summarize(sum_rules.to_a)
    end

    def summarize(name : String, block : TableExpression) : DataFrame
      summarize(ColumnFormula.new(name, block))
    end

    def summarize(name : String, &block : TableExpression) : DataFrame
      summarize(ColumnFormula.new(name, block))
    end

    def summarize_at(&col_sel : ColumnSelector) : DataFrame
      summarize_at(col_sel, [] of AggFunc)
    end

    def summarize_at(col_sel : ColumnSelector, *aggfuns : AggFunc) : DataFrame
      summarize_at(col_sel, aggfuns.to_a)
    end

    def summarize_at(col_sel : ColumnSelector, aggfuns : Array(AggFunc)) : DataFrame
      summarize_at(col_sel, ->(sb : SummarizeBuilder) { aggfuns.each { |f| sb.add(f.value, f.suffix) } })
    end

    def summarize_at(col_sel : ColumnSelector, op : SummarizeFunc? = nil) : DataFrame
      sb = SummarizeBuilder.new(self, col_sel)
      if op
        op.call(sb)
      end
      sb.build
    end

    def take(rows = 5)
      filter { |ec| ec.df.row_number.map(&.<= rows) }
    end

    def take_last(rows : Int32)
      filter { |ec| ec.df.row_number.map(&.>(num_row - rows)) }
    end

    def tail(rows = 5)
      take_last(rows)
    end

    # Create a new dataframe based on a list of column-formulas which are evaluated in the context of the this instance.
    def transmute(*formula : ColumnFormula)
      add_columns(*formula).select(formula.map(&.name))
    end

    # Expose a view on the data as Hash from column names to nullable arrays.
    def to_h
      names.map { |v| {v, self[v].values} }.to_h
    end

    # Converts dataframe to its string representation. This is being invoked via `print` and `to_s`
    def to_string(title = "A DataFrame", col_names = true, max_rows = PRINT_MAX_ROWS,
                  max_width = PRINT_MAX_WIDTH, max_digits = PRINT_MAX_DIGITS,
                  row_numbers = PRINT_ROW_NUMBERS)
      df = self
      df = self.ungroup.as(SimpleDataFrame) unless self.is_a?(SimpleDataFrame)

      max_rows_or_inf = max_rows < 0 ? Int32::MAX : max_rows
      print_data = df.take(Math.min(num_row, max_rows_or_inf))
        .try do |pd|
          # optionally add rownames
          if row_numbers && pd.num_row > 0
            pd.add_column(" ") { |c| c.row_num }.move_left(" ")
          else
            pd
          end
        end
      val_printer = Utils.create_value_printer(max_digits)

      # calculate indents
      col_widths = print_data.cols.map { |c| c.values.map { |v| val_printer.call(v).size }.max? || 20 }
      header_widths = print_data.names.map(&.size)

      # detect column padding
      col_spacing = 3
      padding = col_widths.zip(header_widths).map { |col, head| [col, head].max + col_spacing }
        .tap { |v| v.size > 0 ? (v[0] -= col_spacing) : v } # remove spacer from frist column to have correction alignment with beginning of line

      # do the actual printing
      String.build do |sb|
        sb << "#{title}: #{num_row} x #{num_col}\n"

        sb << "Groups: #{self.by.join(", ")} [#{self.groups.size}]\n" if self.is_a?(GroupedDataFrame)

        # determine which column to actually print to obey width limitations
        num_print_cols = padding.scan_left(0) { |acc, val| acc + val }
          .each_with_index.take_while { |v| v[0] < max_width }.to_a.last[1]

        width_trimmed = print_data.select(print_data.names.first(num_print_cols))

        if (col_names)
          sb << width_trimmed.cols.map_with_index { |col, idx| col.name.pad_start(padding[idx]) }.join << "\n"
        end
        width_trimmed.rows.to_a.map(&.values).map do |row_data|
          # show null as NA when printing data
          sb << row_data.map_with_index { |val, idx| val_printer.call(val).pad_start(padding[idx]) }.join << "\n"
        end

        # similar to dplyr render a summary below the table
        and = Array(String).new

        if (max_rows_or_inf < df.num_row)
          and << "and #{df.num_row - max_rows_or_inf} more rows"
        end

        if (num_print_cols < print_data.num_col)
          left_out_cols = print_data.select(names[num_print_cols..])
          and << "#{print_data.num_col - num_print_cols} more variables: #{left_out_cols.names.join(", ")}"
        end
        sb << and.join(", and ").wrap(max_width)
      end.strip
    end

    def to_s
      to_string
    end

    def to_s(io : Nil)
      io << to_string
    end

    # Save the current dataframe to `separator` delimited file.
    def write_csv(filename : String, separator : Char = ',', quote_char : Char = '"') : Nil
      File.open(filename, "w") do |file|
        write_csv(file, separator, quote_char)
      end
    end

    def write_csv(io : IO, separator : Char = ',', quote_char : Char = '"') : Nil
      io.puts names.join("#{separator}")
      CSV.build(io, separator, quote_char) do |csv|
        row_data.each.each do |drow|
          csv.row do |r|
            drow.each do |v|
              r << (v.raw.nil? ? MISSING_VALUE : v)
            end
          end
        end
      end
    end

    private def self.bind_col_data(dfs : Array(DataFrame), col_name : String)
      total_rows = dfs.map(&.num_row).sum
      list = Array(Any | DataFrame).new(total_rows) { |_| nil }
      return list unless total_rows > 0
      iter = 0
      dfs.each do |df|
        if df.names.includes?(col_name)
          df[col_name].values.each do |val|
            list[iter] = val
            iter += 1
          end
        else
          # column is missing in it
          0.upto(df.num_row - 1) do |_|
            list[iter] = nil
            iter += 1
          end
        end
      end
      list
    end

    private def reduce_col_selectors(which : Array(ColumnSelector)) : ColumnSelector
      which.map { |n| n.call(ColNames.new(names)) }
        .reduce { |a, b| a.and(b) }
        .try { |c| ColumnSelector.new { |_| c } }
    end

    private def col_select_as_names(selector : ColumnSelector)
      validate_column_selector(selector)
      which = selector.call(ColNames.new(names))
      raise CrysdaException.new ("selector array has different dimension than data-frame") unless which.size == self.num_col

      # map bool array to string selection
      pos_sel = which.count { |v| v == true } > 0 || which.select { |v| !v.nil? }.empty?
      which_complete = which.map { |v| v.nil? ? !pos_sel : v }
      names.zip(which_complete).select { |v| v[1] }.map { |v| v[0] }
    end

    private def validate_column_selector(selector : ColumnSelector)
      which = selector.call(ColNames.new(names))
      if which.select { |v| !v.nil? }.uniq.size > 1
        raise InvalidColumnSelectException.new(names, which)
      end
    end

    private def ec : ExpressionContext
      ExpressionContext.new(self)
    end

    protected def row_data
      case self
      when SimpleDataFrame  then DFIter.new(cols)
      when GroupedDataFrame then raise UnSupportedOperationException.new
      else
        raise CrysdaException.new ("Unknown type #{typeof(self)}")
      end
    end
  end

  private struct DuplicateNameResolver
    def initialize(@names : Array(String))
    end

    def resolve(colname : String) : String
      return colname unless colname.in? @names
      1.upto(Int32::MAX) do |suf|
        cname = "#{colname}_#{suf}"
        return cname unless cname.in? @names
      end
      raise CrysdaException.new ("Unable to resolve duplicate column name")
    end
  end

  private class DFIter
    include Iterable(Array(Any))

    def initialize(@cols : Array(DataCol))
    end

    def each
      DFIterator.new(@cols)
    end

    class DFIterator
      include Iterator(Array(AnyVal))
      @col_iters : Array(Iterator(Any | DataFrame))

      def initialize(cols : Array(DataCol))
        @col_iters = Array(Iterator(Any | DataFrame)).new
        cols.each { |c| @col_iters << c.values.each }
      end

      def next
        arr = Array(AnyVal).new
        @col_iters.each do |v|
          val = v.next
          return stop if val.is_a?(Iterator::Stop)
          arr << AnyVal[val]
        end
        arr.empty? ? stop : arr
      end
    end
  end
end
