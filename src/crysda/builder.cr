require "csv"
require "http/client"
require "compress/gzip"
require "json"
require "db"

module Crysda
  # :nodoc:
  def self.get_col_type(col : DataCol, wrap_squares = false)
    val = case (col)
          when Int32Col   then "Int32"
          when Int64Col   then "Int64"
          when Float64Col then "Float64"
          when StringCol  then "String"
          when BoolCol    then "Bool"
          when DFCol      then "DataFrame"
          when AnyCol     then guess_any_type(col)
          else
            raise CrysdaException.new ("Unknown type #{typeof(col)}")
          end
    wrap_squares ? "[#{val}]" : val
  end

  # :nodoc:
  def self.get_scalar_col_type(col : DataCol)
    name = typeof(col).name
    name.lstrip(self.name + "::").rstrip("Col")
  end

  # :nodoc:
  def self.temp_colname
    "temp_col_#{UUID.random}"
  end

  # return column types as an array of `ColSpec` struct
  def self.column_types(df : DataFrame) : Array(ColSpec)
    return column_types(df.ungroup) if df.is_a?(GroupedDataFrame)
    df.cols.map_with_index { |col, idx| ColSpec.new(idx, col.name, get_col_type(col)) }
  end

  private module DataLoader
    extend self

    def read_csv(filename : String, separator : Char = ',', quote_char : Char = '"',
                 skip_blank_lines : Bool = true, skip : Int32 = 0, comment : Char? = '#', header : Int32? = 0,
                 na_value : String = MISSING_VALUE, true_values = ["T", "TRUE"],
                 false_values = ["F", "FALSE"])
      io = get_file_io(filename)
      read_csv(io, separator, quote_char, skip_blank_lines, skip, comment, header, na_value, true_values,
        false_values,
      )
    ensure
      io.try &.close
    end

    def read_csv(io : IO, separator : Char = ',', quote_char : Char = '"',
                 skip_blank_lines : Bool = true, skip : Int32 = 0, comment : Char? = '#', header : Int32? = 0,
                 na_value : String = MISSING_VALUE, true_values = ["T", "TRUE"],
                 false_values = ["F", "FALSE"])
      records = CSV.parse(io, separator, quote_char)
      records = records.reject(&.empty?) if skip_blank_lines
      records = records[skip..]
      if (chr = comment)
        records = records.reject { |row| row[0].starts_with?(chr) }
      end

      if (hdr_row = header)
        raise CrysdaException.new ("Unable to read header at row #{hdr_row}. Total records count : #{records.size}") unless hdr_row < records.size
        colnames = records[hdr_row]
        row_index = hdr_row + 1
      else
        colnames = (1..records[0].size).to_a.map { |index| "Col#{index}" }
        row_index = 0
      end

      cols = Array(DataCol).new
      colnames.each_with_index do |cname, index|
        rows = records[row_index..].map { |r| r[index].na_as_nil(na_value) }
        cols << Utils.get_col(cname, rows, true_values, false_values)
      end

      SimpleDataFrame.new(cols)
    end

    def read_rs(rs : DB::ResultSet)
      colnames = rs.column_names
      data = Hash(String, Array(Any)).new
      colnames.each { |c| data[c] = Array(Any).new }
      rs.each do
        colnames.each do |name|
          case val = rs.read
          when Slice(UInt8) then data[name] << String.new(val)
          when Any          then data[name] << val
          else                   raise CrysdaException.new ("uknown column type : #{val.class}")
          end
        end
      end
      SimpleDataFrame.new(data.map { |c, v| Utils.handle_union(c, v) })
    end

    def read_json(filename : String)
      io = get_file_io(filename)
      read_json(io)
    ensure
      io.try &.close
    end

    def read_json(io : IO)
      json = JSON.parse(io)
      parse_json_array(json.as_a)
    end

    def read_json(json : JSON::Any)
      col_id = "_id"
      df = DataFrameBuilder.new(col_id).values(json)
      is_json = ->(c : DataCol) { Crysda.get_col_type(c).starts_with?("JSON::Any") }
      # convert all json columns
      while df.cols.any? { |c| is_json.call(c) }
        json_col = df.cols.find { |c| is_json.call(c) } || break
        json_col_dfs = json_col.values.map do |val|
          case val
          when JSON::Any
            case val.raw
            when Array then parse_json_array(val.as_a)
            when Hash
              if (fa = val.as_h.values.first?) && fa.raw.is_a?(Array(JSON::Any))
                Crysda.dataframe_of(
                  StringCol.new(json_col.name, val.as_h.keys).as(DataCol),
                  AnyCol.new("value", val.as_h.values).as(DataCol)
                )
              else
                val.as_h.map { |k, v| AnyCol.new(k, [v]).as(DataCol) }
                  .try { |ac| Crysda.dataframe_of(ac) }
                  .add_column(col_id) { |a| a.df.names }
              end
            else
              raise CrysdaException.new ("invalid json or unable to parse json to dataframe")
            end
          else
            raise CrysdaException.new ("Can not parse json.")
          end
        end

        arr_dfs = Array(DataFrame).new(json_col_dfs.size) { |i| json_col_dfs[i] }
        df = df.add_column("_dummy_") { |_| nil }
          .reject(json_col.name)
          .add_column("_json_") { |_| arr_dfs }
          .reject("_dummy_")
          .unnest("_json_")
      end
      df
    end

    private def get_file_io(filename : String)
      tmpname = filename.downcase
      isurl = ["http", "https:", "ftp"].any? { |e| tmpname.starts_with?(e) }
      io = isurl ? read_url(filename) : File.open(filename)
      compressed = ["gz", "gzip", "zip"].any? { |e| tmpname.ends_with?(e) }
      compressed ? Compress::Gzip::Reader.new(io) : io
    end

    private def read_url(url)
      str = ""
      HTTP::Client.get(url) do |resp|
        str = resp.body_io.gets_to_end
      end
      raise CrysdaException.new ("URL returned an empty response") if str.nil? || str.empty?
      IO::Memory.new(str)
    end

    private def parse_json_array(records : Array(JSON::Any))
      col_names = records.map { |v| v.as_h.keys }.reverse.reduce { |acc, right| acc + (right - acc) }
      col_names.map do |name|
        elems = records.first(5).map_with_index { |_, idx| records[idx].as_h[name]? || JSON::Any.new(nil) }
        values = records.map { |h| h[name]? || JSON::Any.new(nil) }.to_a
        case
        when elems.all? { |v| v.raw.is_a?(Int32?) }
          begin
            Int32Col.new(name, Array(Int32?).new(values.size) { |i| values[i].raw.as(Int32?) })
          rescue
            col = get_i64col(name, values) || get_f64col(name, values)
            col || get_strcol(name, values)
          end
        when elems.all? { |v| v.raw.is_a?(Int64?) }   then get_i64col(name, values) || get_f64col(name, values) || get_strcol(name, values)
        when elems.all? { |v| v.raw.is_a?(Float64?) } then get_f64col(name, values) || get_strcol(name, values)
        when elems.all? { |v| v.raw.is_a?(Bool?) }    then get_boolcol(name, values) || get_strcol(name, values)
        when elems.all? { |v| v.raw.is_a?(String?) }  then get_strcol(name, values)
        else                                               AnyCol.new(name, values)
        end
      end.try { |cols| SimpleDataFrame.new(cols) }
    end

    private def get_i64col(name, values)
      Int64Col.new(name, Array(Int64?).new(values.size) { |i| values[i].raw.as(Int64?) }) rescue nil
    end

    private def get_f64col(name, values)
      Float64Col.new(name, Array(Float64?).new(values.size) do |i|
        num = values[i].raw
        if num
          num = num.to_s.gsub(',', "")
          num.to_f
        else
          nil
        end
      end)
    rescue
      nil
    end

    private def get_boolcol(name, values)
      BoolCol.new(name, Array(Bool?).new(values.size) { |i| values[i].as_bool }) rescue nil
    end

    private def get_strcol(name, values)
      StringCol.new(name, Array(String?).new(values.size) { |i| values[i].as_s? })
    end
  end

  private struct DataFrameBuilder
    def initialize(@header : Iterable(String))
    end

    def self.new(*header : String)
      new(header.to_a)
    end

    def self.new(*columns : DataCol)
      new(columns.to_a)
    end

    def self.new(columns : Iterable(DataCol))
      SimpleDataFrame.new(columns)
    end

    def self.new(rows : Iterable(Hash(String, Any)))
      new(rows.map { |r| r.map { |k, v| {k, AnyVal[v]} }.to_h })
    end

    def self.new(rows : Iterable(DataFrameRow))
      new(rows.first.keys.map { |cn| Utils.handle_union(cn, rows.map { |r| r[cn].raw }) })
    end

    def values(*args)
      values(args.to_a)
    end

    def values(args : Array)
      # is values compatible with the header dimension?
      raise CrysdaException.new ("data dimension #{@header.size} is not compatible with the length of values #{args.size}") unless @header.size > 0 && args.size % @header.size == 0
      # break into columns
      raw_cols = args.map_with_index { |a, i| {i % @header.size, a} }
        .group_by { |t| t[0] }
        .values.map { |v| v.map { |t| t[1] } }

      # infer column type by peeking into column data
      table_cols = @header.zip(raw_cols).map { |k, v| Utils.handle_union(k, v) }
      raise CrysdaException.new ("Provided data does not coerce to tabular shape") unless table_cols.map { |c| c.size }.to_set.size == 1
      SimpleDataFrame.new(table_cols)
    end
  end

  private struct SummarizeBuilder
    def initialize(@df : DataFrame, @column_select : ColumnSelector)
      @rules = Hash(SumFormula, String?).new
    end

    def add(how : SumFormula, name : String? = nil, separator : Char = '.')
      @rules[how] = separator.to_s + (name || "")
    end

    def build : DataFrame
      sum_cols = @df.select(&@column_select).names
      if df = @df.as?(GroupedDataFrame)
        df.by.each { |b| sum_cols.delete(b) }
      end

      rules = sum_cols.flat_map do |cname|
        @rules.map do |key, value|
          name = "#{cname}#{(value || key.hash)}"
          ColumnFormula.new(name, TableExpression.new do |ec|
            data_col = ec[cname]
            key.call(data_col)
          end)
        end
      end

      @df.summarize(rules)
    end
  end

  private def self.guess_any_type(col : AnyCol)
    first_el = col.values.reject(&.nil?).first?
    return "Any" if first_el.nil?
    val = first_el.class.name.lstrip(self.name + "::")
    val = val.in?(["SimpleDataFrame", "GroupedDataFrame"]) ? "DataFrame" : val
    val.gsub("ArrayList", "List")
  end
end
