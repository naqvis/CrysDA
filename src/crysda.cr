# CrysDA` is a **{Crys}**tal shard for **{D}**ata **{A}**nalysis. Provides you modern functional-style API for data manipulation to filter, transform, aggregate and reshape tabular data.
# Core of the library is `CrysDA::DataFrame` an immutable data structure interface.
# ## Features
# - [X] Filter, transform, aggregate and reshape tabular data
# - [X] Modern, user-friendly and easy-to-learn data-science API
# - [X] Reads from plain and compressed tsv, csv, json, or any delimited format with or without header from local or remote.
# - [X] Supports grouped operations
# - [X] Supports reading data from DB
# - [X] Tables can contain atomic columns (Number, Float, Bool, String) as well as object columns
# - [X] Reshape tables from wide to long and back
# - [X] Table joins (left, right, semi, inner, outer)
# - [X] Cross tabulation
# - [X] Descriptive statistics (mean, min, max, median, ...)
# - [X] Functional API inspired by [dplyr](http://dplyr.tidyverse.org/), [pandas](http://pandas.pydata.org/)
# - [X] many more...
module Crysda
  VERSION = "0.1.2"

  # reads a comma separated value file/io into a dataframe.
  # `file` could be local file path or a URL. It will read compressed(gz, gzip) files.
  # `separator` defaults to `,` and can be changed to other separator (e.g `\t` for tab separated files)
  # `skip_blank_lines` defaults to true, will skip all blank lines
  # `skip` defaults to 0, will skip this much lines from start of file.
  # `comment` character default `#` will ignore all lines starting with this character
  # `header` line defaults to 0 (first row), if set to `nil` then column names are auto generated starting with `Col1`.
  # if skip_blank_lines and comment are enabled, header will start reading after removing blank and comment lines
  # `na_value` defaults to `NA` Strings which should be treated as `Nil`. values matching this param will be treated as `nil`
  # `true_values` defaults to `["T","TRUE"]` values to consider as boolean true
  # `false_values` defaults to `["F","FALSE"]` values to consider as boolean false
  def self.read_csv(file : String | IO, separator : Char = ',', quote_char : Char = '"',
                    skip_blank_lines : Bool = true, skip : Int32 = 0, comment : Char? = '#', header : Int32? = 0,
                    na_value : String = MISSING_VALUE, true_values = ["T", "TRUE"],
                    false_values = ["F", "FALSE"])
    DataLoader.read_csv(file, separator, quote_char, skip_blank_lines, skip, comment, header, na_value, true_values,
      false_values,
    )
  end

  # reads a json file or URL
  def self.read_json(file : String | IO)
    DataLoader.read_json(file)
  end

  # builds a data-frame from a JSON string
  def self.from_json(json : String)
    DataLoader.read_json(JSON.parse(json))
  end

  # build a data-frame from a `DB::ResultSet`
  def self.from(resultset : DB::ResultSet)
    DataLoader.read_rs(resultset)
  end

  # Creates a new dataframe in place.
  # header - pass headers as variadic parameter
  # call `values` after this call to pass the values
  # ```
  # df = dataframe_of("quarter", "sales", "location").values(1, 300.01, "london", 2, 290, "chicago")
  # ```
  def self.dataframe_of(*header : String)
    DataFrameBuilder.new(*header)
  end

  # Creates a new data-frame from records encoded as key-value maps
  # Column types will be inferred from the value types
  def self.dataframe_of(*rows : DataFrameRow)
    dataframe_of(rows.to_a)
  end

  # Creates a new data-frame from array of `DataFrameRow`
  def self.dataframe_of(rows : Iterable(DataFrameRow))
    DataFrameBuilder.new(rows)
  end

  # Creates a new data-frame from `{} of String => Any`
  def self.dataframe_of(*rows : Hash(String, Any))
    dataframe_of(rows.to_a)
  end

  # Creates a new data-frame from Array of `{} of String => Any`
  def self.dataframe_of(rows : Iterable(Hash(String, Any)))
    DataFrameBuilder.new(rows)
  end

  # Create a new data-frame from a list of `DataCol` instances
  def self.dataframe_of(*cols : DataCol)
    SimpleDataFrame.new(*cols)
  end

  # Creates a data-frame from Array of `DataCol`
  def self.dataframe_of(cols : Iterable(DataCol))
    SimpleDataFrame.new(cols)
  end

  # Creates an empty dataframe with 0 observation
  def self.empty_df
    DataFrame.empty
  end

  # helper method to return the block as `Proc`. Used when doing select with multiple criteria.
  # Kind of workaround as Crystal doesn't allow variadic blocks and `Proc` definition requires
  # complete signature like `Crysda::ColumnSelector.new{|e| ....}`
  # so instead of
  # ```
  # df.select(
  #   Crysda::ColumnSelector.new { |s| ... },
  #   Crysda::ColumnSelector.new { |s| ... }
  # )
  # ```
  # One can simply use this helper
  # ```
  # df.select(
  #  Crysda.selector{|e| ....},
  #  Crysda.selector{|e| ....},
  # )
  # ```
  def self.selector(&block : ColumnSelector)
    block
  end

  # Adds new rows. Missing entries are set to null. The output of bind_rows will contain a column if that column appears in any of the inputs.
  # When row-binding, columns are matched by name, and any missing columns will be filled with NA.
  # Grouping will be discarded when binding rows
  def self.bind_rows(*dfs : DataFrame) : DataFrame
    DataFrame.bind_rows(dfs.to_a)
  end

  # Binds dataframes by column. Rows are matched by position, so all data frames must have the same number of rows.
  def self.bind_cols(left : DataFrame, right : DataFrame, rename_duplicates = true) : DataFrame
    DataFrame.bind_cols(left, right, rename_duplicates)
  end
end

require "./**"
