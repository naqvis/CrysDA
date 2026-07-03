require "csv"
require "http/client"
require "compress/gzip"
require "json"
require "db"

module Crysda
  # :nodoc:
  def self.get_col_type(col : DataCol, wrap_squares = false)
    val = case col
          when Int32Col      then "Int32"
          when Int64Col      then "Int64"
          when Float64Col    then "Float64"
          when StringCol     then "String"
          when BoolCol       then "Bool"
          when DateTimeCol   then "DateTime"
          when TimestampCol  then "Timestamp"
          when BigDecimalCol then "BigDecimal"
          when DFCol         then "DataFrame"
          when AnyCol        then guess_any_type(col)
          else
            raise CrysdaException.new("Unknown type #{typeof(col)}")
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

  # Converter registry for DB driver types not in Crysda::Any
  # Register a converter that receives the value as String and returns Any:
  #   Crysda.register_converter("PG::Numeric") { |s| s.to_f64 }
  @@converters = {} of String => Proc(String, Any)

  def self.register_converter(type_name : String, &block : String -> Any)
    @@converters[type_name] = block
  end

  def self.converter_for(type_name : String) : Proc(String, Any)?
    @@converters[type_name]?
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
      if chr = comment
        records = records.reject(&.[0].starts_with?(chr))
      end

      if hdr_row = header
        raise CrysdaException.new("Unable to read header at row #{hdr_row}. Total records count : #{records.size}") unless hdr_row < records.size
        colnames = records[hdr_row]
        row_index = hdr_row + 1
      else
        colnames = (1..records[0].size).to_a.map { |index| "Col#{index}" }
        row_index = 0
      end

      data_records = records[row_index..]
      num_rows = data_records.size

      cols = Array(DataCol).new
      colnames.each_with_index do |cname, col_idx|
        cols << build_column_optimized(cname, data_records, col_idx, na_value, true_values, false_values)
      end

      SimpleDataFrame.new(cols)
    end

    # Build column directly with Slice + NullBitmap - avoids intermediate Array(T?)
    private def build_column_optimized(name : String, records : Array(Array(String)), col_idx : Int32,
                                       na_value : String, true_values : Array(String), false_values : Array(String)) : DataCol
      num_rows = records.size
      return StringCol.new(name, [] of String?) if num_rows == 0

      # Sample first 20 rows to determine type
      sample_size = Math.min(20, num_rows)
      samples = Array(String?).new(sample_size) { |i| records[i][col_idx].na_as_nil(na_value) }

      t_vals = true_values.map(&.upcase)
      f_vals = false_values.map(&.upcase)

      # Try to build optimized columns directly
      if precision = epoch_samples?(samples)
        build_epoch_col(name, records, col_idx, na_value, precision) ||
          build_string_col_direct(name, records, col_idx, na_value)
      elsif int32col_samples?(samples)
        build_int32_col_direct(name, records, col_idx, na_value) ||
          build_int64_col_direct(name, records, col_idx, na_value) ||
          build_float64_col_direct(name, records, col_idx, na_value) ||
          build_string_col_direct(name, records, col_idx, na_value)
      elsif int64col_samples?(samples)
        build_int64_col_direct(name, records, col_idx, na_value) ||
          build_float64_col_direct(name, records, col_idx, na_value) ||
          build_string_col_direct(name, records, col_idx, na_value)
      elsif float64col_samples?(samples)
        if big_decimal_samples?(samples)
          build_big_decimal_col_direct(name, records, col_idx, na_value) ||
            build_float64_col_direct(name, records, col_idx, na_value) ||
            build_string_col_direct(name, records, col_idx, na_value)
        else
          build_float64_col_direct(name, records, col_idx, na_value) ||
            build_string_col_direct(name, records, col_idx, na_value)
        end
      elsif boolcol_samples?(samples, t_vals, f_vals)
        build_bool_col_direct(name, records, col_idx, na_value, t_vals, f_vals)
      elsif datetime_samples?(samples)
        prec = datetime_precision(samples)
        if prec.in?(:microseconds, :nanoseconds)
          build_timestamp_col_direct(name, records, col_idx, na_value) ||
            build_string_col_direct(name, records, col_idx, na_value)
        else
          build_datetime_col_direct(name, records, col_idx, na_value) ||
            build_string_col_direct(name, records, col_idx, na_value)
        end
      else
        build_string_col_direct(name, records, col_idx, na_value)
      end
    end

    # Build Int32Col directly with Slice + NullBitmap
    private def build_int32_col_direct(name : String, records : Array(Array(String)), col_idx : Int32, na_value : String) : Int32Col?
      num_rows = records.size
      data = Slice(Int32).new(num_rows, 0)
      bitmap = NullBitmap.new(num_rows)

      num_rows.times do |i|
        val = records[i][col_idx]
        if val == na_value || val.empty?
          bitmap.set(i)
        else
          begin
            data[i] = val.to_i32
          rescue
            return nil # Fall back to next type
          end
        end
      end

      Int32Col.new(name, data, bitmap)
    end

    # Build Int64Col directly with Slice + NullBitmap
    private def build_int64_col_direct(name : String, records : Array(Array(String)), col_idx : Int32, na_value : String) : Int64Col?
      num_rows = records.size
      data = Slice(Int64).new(num_rows, 0_i64)
      bitmap = NullBitmap.new(num_rows)

      num_rows.times do |i|
        val = records[i][col_idx]
        if val == na_value || val.empty?
          bitmap.set(i)
        else
          begin
            data[i] = val.to_i64
          rescue
            return nil
          end
        end
      end

      Int64Col.new(name, data, bitmap)
    end

    # Build Float64Col directly with Slice + NullBitmap
    private def build_float64_col_direct(name : String, records : Array(Array(String)), col_idx : Int32, na_value : String) : Float64Col?
      num_rows = records.size
      data = Slice(Float64).new(num_rows, 0.0)
      bitmap = NullBitmap.new(num_rows)

      num_rows.times do |i|
        val = records[i][col_idx]
        if val == na_value || val.empty?
          bitmap.set(i)
        else
          begin
            # Handle comma as thousands separator
            clean_val = val.gsub(',', "")
            data[i] = clean_val.to_f64
          rescue
            return nil
          end
        end
      end

      Float64Col.new(name, data, bitmap)
    end

    # Build BigDecimalCol directly with Slice + NullBitmap
    private def build_big_decimal_col_direct(name : String, records : Array(Array(String)), col_idx : Int32, na_value : String) : BigDecimalCol?
      num_rows = records.size
      data = Slice(BigDecimal).new(num_rows, BigDecimal.new(0))
      bitmap = NullBitmap.new(num_rows)

      num_rows.times do |i|
        val = records[i][col_idx]
        if val == na_value || val.empty?
          bitmap.set(i)
        else
          begin
            clean_val = val.gsub(',', "")
            data[i] = BigDecimal.new(clean_val)
          rescue
            return nil
          end
        end
      end

      BigDecimalCol.new(name, data, bitmap)
    end

    # Build StringCol directly with Array + NullBitmap
    # Uses string interning for categorical data (low cardinality)
    private def build_string_col_direct(name : String, records : Array(Array(String)), col_idx : Int32, na_value : String) : StringCol
      num_rows = records.size
      return StringCol.new(name, [] of String?) if num_rows == 0

      # First pass: collect unique values to determine if categorical
      pool = StringPool.new
      data = Array(String).new(num_rows, "")
      bitmap = NullBitmap.new(num_rows)

      num_rows.times do |i|
        val = records[i][col_idx]
        if val == na_value
          bitmap.set(i)
        else
          # Intern strings to save memory for repeated values
          data[i] = pool.intern(val)
        end
      end

      StringCol.new(name, data, bitmap)
    end

    # Build BoolCol directly
    private def build_bool_col_direct(name : String, records : Array(Array(String)), col_idx : Int32,
                                      na_value : String, t_vals : Array(String), f_vals : Array(String)) : BoolCol
      num_rows = records.size
      data = Array(Bool?).new(num_rows)

      num_rows.times do |i|
        val = records[i][col_idx]
        if val == na_value || val.empty?
          data << nil
        else
          uval = val.upcase
          if uval.in?(t_vals)
            data << true
          elsif uval.in?(f_vals)
            data << false
          else
            data << nil
          end
        end
      end

      BoolCol.new(name, data)
    end

    # Type detection helpers for samples
    private def int32col_samples?(samples)
      samples.all? { |v| v.nil? || v.try(&.to_i32) rescue false }
    end

    private def int64col_samples?(samples)
      samples.all? { |v| v.nil? || v.try(&.to_i64) rescue false }
    end

    private def float64col_samples?(samples)
      samples.all? { |v| v.nil? || v.try { |s| s.gsub(',', "").to_f64 } rescue false }
    end

    # Detect if samples lose precision as Float64 (BigDecimal round-trip check)
    private def big_decimal_samples?(samples)
      samples.any? do |v|
        next false if v.nil?
        f64 = v.gsub(',', "").to_f64?
        next false if f64.nil?
        begin
          BigDecimal.new(v) != BigDecimal.new(f64.to_s)
        rescue
          false
        end
      end
    end

    private def boolcol_samples?(samples, t_vals, f_vals)
      samples.all? do |v|
        next true if v.nil?
        uv = v.upcase
        uv.in?(t_vals) || uv.in?(f_vals)
      end
    end

    private def datetime_samples?(samples)
      samples.all? do |v|
        next true if v.nil?
        parse_datetime_sample(v) != nil
      end
    end

    private def parse_datetime_sample(s : String) : Time?
      DateTimeCol::DATETIME_MS_FORMATS.each do |fmt|
        begin
          return Time.parse(s, fmt, Time::Location::UTC)
        rescue
          next
        end
      end
      DateTimeCol::DATETIME_FORMATS.each do |fmt|
        begin
          return Time.parse(s, fmt, Time::Location::UTC)
        rescue
          next
        end
      end
      nil
    end

    # Detect datetime precision from fractional digit count
    private def datetime_precision(samples : Array(String?)) : Symbol
      max = :seconds
      samples.each do |v|
        next if v.nil?
        if m = v.match(/\.(\d+)/)
          case m[1].size
          when 1..3 then max = :milliseconds if max == :seconds
          when 4..6 then max = :microseconds
          when 7..9 then max = :nanoseconds
          end
        end
      end
      max
    end

    EPOCH_DIGITS = {10 => :seconds, 13 => :milliseconds, 16 => :microseconds, 19 => :nanoseconds}

    # Detect if samples are integer epoch timestamps
    private def epoch_samples?(samples : Array(String?)) : Symbol?
      precisions = Set(Symbol).new
      samples.each do |v|
        next if v.nil?
        int = v.to_i64?
        return nil if int.nil?
        digits = v.size
        digits -= 1 if v.starts_with?('-')
        if prec = EPOCH_DIGITS[digits]?
          precisions << prec
        else
          return nil
        end
      end
      return nil if precisions.empty?
      # Return highest precision present (mixed → use most precise)
      precisions.max
    end

    # Build DateTimeCol or TimestampCol from integer epoch values
    private def build_epoch_col(name : String, records : Array(Array(String)), col_idx : Int32, na_value : String, precision : Symbol) : DataCol?
      num_rows = records.size
      bitmap = NullBitmap.new(num_rows)

      case precision
      when :seconds
        data = Slice(Int64).new(num_rows, 0_i64)
        num_rows.times do |i|
          val = records[i][col_idx]
          if val == na_value || val.empty?
            bitmap.set(i)
          else
            data[i] = val.to_i64 * 1000
          end
        end
        DateTimeCol.new(name, data, bitmap)
      when :milliseconds
        data = Slice(Int64).new(num_rows, 0_i64)
        num_rows.times do |i|
          val = records[i][col_idx]
          if val == na_value || val.empty?
            bitmap.set(i)
          else
            data[i] = val.to_i64
          end
        end
        DateTimeCol.new(name, data, bitmap)
      when :microseconds
        data = Slice(Int128).new(num_rows, Int128.new(0))
        num_rows.times do |i|
          val = records[i][col_idx]
          if val == na_value || val.empty?
            bitmap.set(i)
          else
            data[i] = val.to_i128 * 1000_i128
          end
        end
        TimestampCol.new(name, data, bitmap)
      when :nanoseconds
        data = Slice(Int128).new(num_rows, Int128.new(0))
        num_rows.times do |i|
          val = records[i][col_idx]
          if val == na_value || val.empty?
            bitmap.set(i)
          else
            data[i] = val.to_i128
          end
        end
        TimestampCol.new(name, data, bitmap)
      end
    end

    # Build TimestampCol from nanosecond-precision ISO strings
    private def build_timestamp_col_direct(name : String, records : Array(Array(String)), col_idx : Int32, na_value : String) : TimestampCol?
      num_rows = records.size
      data = Slice(Int128).new(num_rows, Int128.new(0))
      bitmap = NullBitmap.new(num_rows)

      detected_format : String? = nil
      num_rows.times do |i|
        val = records[i][col_idx]
        next if val == na_value || val.empty?
        TimestampCol::NANO_FORMATS.each do |fmt|
          begin
            Time.parse(val, fmt, Time::Location::UTC)
            detected_format = fmt
            break
          rescue
            next
          end
        end
        break if detected_format
      end

      return nil unless detected_format

      num_rows.times do |i|
        val = records[i][col_idx]
        if val == na_value || val.empty?
          bitmap.set(i)
        else
          begin
            time = Time.parse(val, detected_format, Time::Location::UTC)
            data[i] = time.to_unix_ns
          rescue
            return nil
          end
        end
      end

      TimestampCol.new(name, data, bitmap)
    end

    # Build DateTimeCol directly with Slice + NullBitmap
    private def build_datetime_col_direct(name : String, records : Array(Array(String)), col_idx : Int32, na_value : String) : DateTimeCol?
      num_rows = records.size
      data = Slice(Int64).new(num_rows, 0_i64)
      bitmap = NullBitmap.new(num_rows)

      # Detect format from first non-null value
      detected_format : String? = nil
      num_rows.times do |i|
        val = records[i][col_idx]
        next if val == na_value || val.empty?
        DateTimeCol::DATETIME_MS_FORMATS.each do |fmt|
          begin
            Time.parse(val, fmt, Time::Location::UTC)
            detected_format = fmt
            break
          rescue
            next
          end
        end
        unless detected_format
          DateTimeCol::DATETIME_FORMATS.each do |fmt|
            begin
              Time.parse(val, fmt, Time::Location::UTC)
              detected_format = fmt
              break
            rescue
              next
            end
          end
        end
        break if detected_format
      end

      return nil unless detected_format

      num_rows.times do |i|
        val = records[i][col_idx]
        if val == na_value || val.empty?
          bitmap.set(i)
        else
          begin
            time = Time.parse(val, detected_format, Time::Location::UTC)
            data[i] = time.to_unix_ms
          rescue
            return nil # Fall back to string
          end
        end
      end

      DateTimeCol.new(name, data, bitmap)
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
          else
            type_name = typeof(val).name
            repr = val.to_s
            if converter = Crysda.converter_for(type_name)
              data[name] << converter.call(repr)
            else
              data[name] << repr
            end
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
                  .add_column(col_id, &.df.names)
              end
            else
              raise CrysdaException.new("invalid json or unable to parse json to dataframe")
            end
          else
            raise CrysdaException.new("Can not parse json.")
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
      raise CrysdaException.new("URL returned an empty response") if str.nil? || str.empty?
      IO::Memory.new(str)
    end

    private def parse_json_array(records : Array(JSON::Any))
      col_names = records.map(&.as_h.keys).reverse!.reduce { |acc, right| acc + (right - acc) }
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
      new(rows.map(&.map { |k, v| {k, AnyVal[v]} }.to_h))
    end

    def self.new(rows : Iterable(DataFrameRow))
      new(rows.first.keys.map { |cn| Utils.handle_union(cn, rows.map(&.[cn].raw)) })
    end

    def values(*args)
      values(args.to_a)
    end

    def values(args : Array)
      # is values compatible with the header dimension?
      raise CrysdaException.new("data dimension #{@header.size} is not compatible with the length of values #{args.size}") unless @header.size > 0 && args.size % @header.size == 0
      # break into columns
      raw_cols = args.map_with_index { |a, i| {i % @header.size, a} }
        .group_by { |t| t[0] }
        .values.map { |v| v.map { |t| t[1] } }

      # infer column type by peeking into column data
      table_cols = @header.zip(raw_cols).map { |k, v| Utils.handle_union(k, v) }
      raise CrysdaException.new("Provided data does not coerce to tabular shape") unless table_cols.map(&.size).to_set.size == 1
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
    first_el = col.values.reject(Nil).first?
    return "Any" if first_el.nil?
    val = first_el.class.name.lstrip(self.name + "::")
    val = val.in?(["SimpleDataFrame", "GroupedDataFrame"]) ? "DataFrame" : val
    val.gsub("ArrayList", "List")
  end
end
