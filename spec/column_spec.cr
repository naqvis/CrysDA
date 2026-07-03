require "./spec_helper"

module Crysda
  private def self.pct_change_for(df, prod, col)
    df.filter { |v| v["product"] == prod }[col + "_pct_change"].values
  end

  describe Crysda do
    it "it should do correct arithmetics" do
      (Int32Col.new("", [3]) + 3)[0].should eq(6)
      (Int32Col.new("", [3]) + 3.0)[0].should eq(6.0)
      (Int32Col.new("", [3]) + "foo")[0].should eq("3foo")

      (Int64Col.new("", [3_i64]) + 3)[0].should eq(6)
      (Int64Col.new("", [3_i64]) + 3.0)[0].should eq(6.0)
      (Int64Col.new("", [3_i64]) + "foo")[0].should eq("3foo")

      (Float64Col.new("", [3.0]) + 3)[0].should eq(6)
      (Float64Col.new("", [3.0]) + 3.0)[0].should eq(6.0)
      (Float64Col.new("", [3.0]) + "foo")[0].should eq("3.0foo")

      (BoolCol.new("foo", [false, true]))[1].should eq(true)

      (StringCol.new("", ["3"]) + "foo")[0].should eq("3foo")
      expect_raises(UnSupportedOperationException) do
        (StringCol.new("", ["3"]) + 3)[0].should eq(6)
      end
    end

    it "allow to negate and invert columns" do
      (-Int32Col.new("", [1, 2]))[1].should eq(-2)
      (-Int64Col.new("", [1i64, 2i64]))[1].should eq(-2i64)
      (-Float64Col.new("", [1.0, 2.0]))[1].should eq(-2.0)
      expect_raises(UnSupportedOperationException) do
        (-AnyCol.new("", [1]))[0].should eq(-1)
      end
    end

    it "allow to create new column conditionally" do
      df = DataFrameBuilder.new("first_name", "last_name", "age", "weight", "adult").values(
        "Max", "Doe", 23, 55.8, true,
        "Franz", "Smith", 23, 88.3, true,
        "Horst", "Keanes", 12, 82.5, false,
      )
      df.add_columns(
        "initials".with { |v| v["first_name"].map { |n| n.to_s[0] }.concatenate(v["last_name"].map { |n| n.to_s[0] }) }
      )

      df.add_column("foo") do |ec|
        ec.where(ec["weight"].as_f64.map_non_nil { |v| v.not_nil!.abs > 60 }.nil_as_false, "over", "normal")
      end
    end

    it "compare columns correctly" do
      df = DataFrameBuilder.new("a", "b").values(1, 1.5, 3, 2.5, 4, 4.0)
      df.add_column("foo") { |ec| ec["a"] > ec["b"] }["foo"].values.should eq([false, true, false])
      df.add_column("foo") { |ec| ec["a"] >= ec["b"] }["foo"].values.should eq([false, true, true])
    end

    it "calculate cummulative sum in grouped dataframe including NAs" do
      sales = DataFrameBuilder.new("product", "sales").values(
        "A", 32.3,
        "A", 12.2,
        "A", 24.2,
        "B", 23.3,
        "B", 12.3,
        "B", nil,
        "B", 2.5
      )

      cumsum_grp = sales.group_by("product").add_column("cum_sales".with(&.["sales"].cumsum))
      cumsum_grp.tap do |grp|
        grp.num_row.should eq(sales.num_row)
        grp["cum_sales"][1].should eq(44.5)
        grp["cum_sales"][4].should eq(35.6)
        grp["cum_sales"][5].should eq(nil)
        grp["cum_sales"][6].should eq(nil)
      end
    end

    it "calculate percentage change in grouped dataframe including NAs" do
      sales = DataFrameBuilder.new("product", "sales", "price").values(
        "A", nil, nil,
        "A", 10, 0.1,
        "A", 50, 0.5,
        "A", 10, 0.1,
        "B", 100, 1.0,
        "B", 150, 1.5,
        "B", nil, nil,
        "B", 75, 0.75
      )

      pct_chng = sales.group_by("product")
        .add_column("sales_pct_change".with(&.["sales"].pct_change))
        .add_column("price_pct_change".with(&.["price"].pct_change))

      pct_chng.tap do |df|
        df.num_row.should eq(sales.num_row)
        pct_change_for(pct_chng, "A", "sales").should eq([nil, nil, 4.0, -0.8])
        pct_change_for(pct_chng, "A", "price").should eq([nil, nil, 4.0, -0.8])
        pct_change_for(pct_chng, "B", "sales").should eq([nil, 0.5, nil, nil])
        pct_change_for(pct_chng, "B", "price").should eq([nil, 0.5, nil, nil])
      end
    end

    it "calculate lead and lag values" do
      sales = DataFrameBuilder.new("sales", "price").values(
        10, 0.1,
        20, 0.2,
        nil, nil,
        40, 0.4,
        50, 0.5
      )

      lead_lag = sales
        .add_column("sales_lead".with(&.["sales"].lead))
        .add_column("price_lag".with(&.["price"].lag(n: 2)))

      lead_lag.tap do |df|
        df.num_row.should eq(sales.num_row)
        df["sales_lead"].values.should eq([20, nil, 40, 50, nil])
        df["price_lag"].values.should eq([nil, nil, 0.1, 0.2, nil])
      end
    end

    it "lead lag column arithmetics" do
      sales = DataFrameBuilder.new("quarter", "sales", "store").values(
        1, 30, "london",
        2, 10, "london",
        3, 50, "london",
        4, 10, "london",
        1, 100, "berlin",
        2, 150, "berlin",
        3, nil, "berlin",
        4, 75, "berlin"
      )

      sales.group_by("store")
        .add_column("quarter_diff".with { |v| v["sales"] - v["sales"].lag(n: 1) })
        .tap do |df|
          df.num_row.should eq(sales.num_row)
          df["quarter_diff"][0].should eq(nil)
          df["quarter_diff"][1].should eq(-20)
        end

      sales.group_by("store")
        .add_column("lookahead_diff".with { |v| v["sales"] - v["sales"].lead(n: 1) })
        .tap do |df|
          df.num_row.should eq(sales.num_row)
          df["lookahead_diff"][0].should eq(20)
        end
    end

    it "ensure custom defaults are added when using lead lag" do
      sales = DataFrameBuilder.new("quarter", "sales", "store").values(
        1, 30, "london",
        2, 10, "london",
        3, 50, "london",
        4, 10, "london",
        1, 100, "berlin",
        2, 150, "berlin",
        3, nil, "berlin",
        4, 75, "berlin"
      )
      sales.add_column("lagged".with(&.["store"].lead(n: 1, default: "bla")))
        .tap do |df|
          df["lagged"][-1].should eq("bla")
        end
      # test numeric (with int default to add a bit complexity)
      sales.add_column("lagged".with(&.["quarter"].lead(default: 42)))
        .tap do |df|
          df["lagged"][-1].should eq(42)
        end

      # test Any Column
      df = DataFrameBuilder.new("uuid").values(
        UUID.random,
        UUID.random,
        UUID.random
      )
      df.add_column("prev_uuid".with(&.["uuid"].lag(default: "foo")))
        .tap do |v|
          v["prev_uuid"][0].should eq("foo")
        end

      uuid = UUID.random
      df.add_column("prev_uuid".with(&.["uuid"].lag(default: uuid)))
        .tap do |v|
          v["prev_uuid"][0].should eq(uuid)
        end
    end

    # DateTimeCol tests
    describe DateTimeCol do
      it "creates DateTimeCol from Time array" do
        times = [
          Time.utc(2023, 1, 15, 10, 30, 0),
          Time.utc(2023, 6, 20, 14, 45, 30),
          nil,
          Time.utc(2023, 12, 31, 23, 59, 59),
        ]
        col = DateTimeCol.new("dates", times)
        col.size.should eq(4)
        col.has_nulls?.should be_true
        col[0].should eq(Time.utc(2023, 1, 15, 10, 30, 0))
        col[2].should be_nil
      end

      it "parses datetime strings with auto-detection" do
        strings = ["2023-01-15", "2023-06-20", nil, "2023-12-31"]
        col = DateTimeCol.parse("dates", strings)
        col.size.should eq(4)
        col[0].not_nil!.year.should eq(2023)
        col[0].not_nil!.month.should eq(1)
        col[0].not_nil!.day.should eq(15)
        col[2].should be_nil
      end

      it "parses datetime strings with explicit format" do
        strings = ["15/01/2023", "20/06/2023", "31/12/2023"]
        col = DateTimeCol.parse("dates", strings, "%d/%m/%Y")
        col[0].not_nil!.day.should eq(15)
        col[0].not_nil!.month.should eq(1)
        col[1].not_nil!.month.should eq(6)
      end

      it "extracts year component" do
        times = [Time.utc(2020, 1, 1), Time.utc(2021, 6, 15), Time.utc(2022, 12, 31)]
        col = DateTimeCol.new("dates", times)
        years = col.year
        years[0].should eq(2020)
        years[1].should eq(2021)
        years[2].should eq(2022)
      end

      it "extracts month component" do
        times = [Time.utc(2023, 1, 1), Time.utc(2023, 6, 15), Time.utc(2023, 12, 31)]
        col = DateTimeCol.new("dates", times)
        months = col.month
        months[0].should eq(1)
        months[1].should eq(6)
        months[2].should eq(12)
      end

      it "extracts day component" do
        times = [Time.utc(2023, 1, 1), Time.utc(2023, 6, 15), Time.utc(2023, 12, 31)]
        col = DateTimeCol.new("dates", times)
        days = col.day
        days[0].should eq(1)
        days[1].should eq(15)
        days[2].should eq(31)
      end

      it "extracts hour, minute, second components" do
        times = [Time.utc(2023, 1, 1, 10, 30, 45), Time.utc(2023, 1, 1, 23, 59, 0)]
        col = DateTimeCol.new("dates", times)
        col.hour[0].should eq(10)
        col.hour[1].should eq(23)
        col.minute[0].should eq(30)
        col.minute[1].should eq(59)
        col.second[0].should eq(45)
        col.second[1].should eq(0)
      end

      it "extracts day_of_week" do
        # Crystal's DayOfWeek: Monday=1, Tuesday=2, ..., Sunday=7
        times = [Time.utc(2023, 1, 1), Time.utc(2023, 1, 2), Time.utc(2023, 1, 7)]
        col = DateTimeCol.new("dates", times)
        dow = col.day_of_week
        dow[0].should eq(7) # Sunday
        dow[1].should eq(1) # Monday
        dow[2].should eq(6) # Saturday
      end

      it "extracts day_of_year" do
        times = [Time.utc(2023, 1, 1), Time.utc(2023, 2, 1), Time.utc(2023, 12, 31)]
        col = DateTimeCol.new("dates", times)
        doy = col.day_of_year
        doy[0].should eq(1)
        doy[1].should eq(32)
        doy[2].should eq(365)
      end

      it "calculates min and max" do
        times = [Time.utc(2023, 6, 15), Time.utc(2020, 1, 1), Time.utc(2025, 12, 31)]
        col = DateTimeCol.new("dates", times)
        col.min.should eq(Time.utc(2020, 1, 1))
        col.max.should eq(Time.utc(2025, 12, 31))
      end

      it "handles min/max with nulls" do
        times = [Time.utc(2023, 6, 15), nil, Time.utc(2020, 1, 1)]
        col = DateTimeCol.new("dates", times)
        expect_raises(MissingValueException) { col.min }
        col.min(remove_na: true).should eq(Time.utc(2020, 1, 1))
        col.max(remove_na: true).should eq(Time.utc(2023, 6, 15))
      end

      it "compares with Time values" do
        times = [Time.utc(2020, 1, 1), Time.utc(2023, 6, 15), Time.utc(2025, 12, 31)]
        col = DateTimeCol.new("dates", times)
        threshold = Time.utc(2023, 1, 1)
        (col > threshold).should eq([false, true, true])
        (col >= threshold).should eq([false, true, true])
        (col < threshold).should eq([true, false, false])
        (col <= threshold).should eq([true, false, false])
      end

      it "formats with strftime" do
        times = [Time.utc(2023, 1, 15, 10, 30, 0), Time.utc(2023, 12, 31, 23, 59, 59)]
        col = DateTimeCol.new("dates", times)
        formatted = col.strftime("%Y-%m-%d")
        formatted[0].should eq("2023-01-15")
        formatted[1].should eq("2023-12-31")
      end

      it "supports lazy iteration" do
        times = [Time.utc(2023, 1, 1), nil, Time.utc(2023, 12, 31)]
        col = DateTimeCol.new("dates", times)

        # each
        collected = [] of Time?
        col.each { |v| collected << v }
        collected.size.should eq(3)
        collected[1].should be_nil

        # each_non_null
        non_null = [] of Time
        col.each_non_null { |v| non_null << v }
        non_null.size.should eq(2)
      end

      it "creates from epoch seconds" do
        epochs = [1672531200_i64, 1687219200_i64, nil] # 2023-01-01, 2023-06-20
        col = DateTimeCol.from_epoch("dates", epochs)
        col[0].not_nil!.year.should eq(2023)
        col[2].should be_nil
      end

      it "creates from epoch milliseconds" do
        epochs = [1672531200000_i64, 1687219200000_i64, nil]
        col = DateTimeCol.from_epoch_ms("dates", epochs)
        col[0].not_nil!.year.should eq(2023)
        col[2].should be_nil
      end

      it "preserves millisecond precision" do
        times = [
          Time.utc(2023, 1, 15, 10, 30, 45, nanosecond: 123_000_000),
          Time.utc(2023, 6, 20, 14, 45, 30, nanosecond: 987_000_000),
        ]
        col = DateTimeCol.new("dates", times)
        col.millisecond[0].should eq(123)
        col.millisecond[1].should eq(987)
      end

      it "parses millisecond format strings" do
        strings = ["2023-01-15 10:30:45.123", "2023-06-20 14:45:30.987"]
        col = DateTimeCol.parse("dates", strings)
        col.millisecond[0].should eq(123)
        col.millisecond[1].should eq(987)
      end

      it "supports lead/lag" do
        times = [Time.utc(2023, 1, 1), Time.utc(2023, 1, 2), Time.utc(2023, 1, 3)]
        col = DateTimeCol.new("dates", times)
        col.lead(1)[0].should eq(Time.utc(2023, 1, 2))
        col.lead(1)[2].should be_nil
        col.lag(1)[0].should be_nil
        col.lag(1)[1].should eq(Time.utc(2023, 1, 1))
      end

      it "supports ffill/bfill" do
        times = [Time.utc(2023, 1, 1), nil, Time.utc(2023, 1, 3)]
        col = DateTimeCol.new("dates", times)
        col.ffill[0].should eq(Time.utc(2023, 1, 1))
        col.ffill[1].should eq(Time.utc(2023, 1, 1))
        col.ffill[2].should eq(Time.utc(2023, 1, 3))
        col.bfill[0].should eq(Time.utc(2023, 1, 1))
        col.bfill[1].should eq(Time.utc(2023, 1, 3))
        col.bfill[2].should eq(Time.utc(2023, 1, 3))
      end
    end

    # TimestampCol tests
    describe TimestampCol do
      it "creates TimestampCol from Time array" do
        times = [
          Time.utc(2023, 1, 15, 10, 30, 45, nanosecond: 123_456_789),
          Time.utc(2023, 6, 20, 14, 45, 30, nanosecond: 987_654_321),
          nil,
        ]
        col = TimestampCol.new("ts", times)
        col.size.should eq(3)
        col.has_nulls?.should be_true
        col[0].should eq(Time.utc(2023, 1, 15, 10, 30, 45, nanosecond: 123_456_789))
        col[2].should be_nil
      end

      it "parses nanosecond format strings" do
        strings = ["2023-01-15 10:30:45.123456789", "2023-06-20 14:45:30.987654321"]
        col = TimestampCol.parse("ts", strings)
        col.nanosecond[0].should eq(123456789)
        col.nanosecond[1].should eq(987654321)
      end

      it "extracts year, month, day components" do
        times = [Time.utc(2020, 1, 1), Time.utc(2022, 12, 31)]
        col = TimestampCol.new("ts", times)
        col.year[0].should eq(2020)
        col.year[1].should eq(2022)
        col.month[1].should eq(12)
        col.day[1].should eq(31)
      end

      it "extracts hour, minute, second, millisecond, microsecond, nanosecond" do
        times = [Time.utc(2023, 1, 1, 10, 30, 45, nanosecond: 123_456_789)]
        col = TimestampCol.new("ts", times)
        col.hour[0].should eq(10)
        col.minute[0].should eq(30)
        col.second[0].should eq(45)
        col.millisecond[0].should eq(123)
        col.microsecond[0].should eq(123456)
        col.nanosecond[0].should eq(123456789)
      end

      it "calculates min and max" do
        times = [Time.utc(2023, 6, 15), Time.utc(2020, 1, 1), Time.utc(2025, 12, 31)]
        col = TimestampCol.new("ts", times)
        col.min.should eq(Time.utc(2020, 1, 1))
        col.max.should eq(Time.utc(2025, 12, 31))
      end

      it "handles min/max with nulls" do
        times = [Time.utc(2023, 6, 15), nil, Time.utc(2020, 1, 1)]
        col = TimestampCol.new("ts", times)
        expect_raises(MissingValueException) { col.min }
        col.min(remove_na: true).should eq(Time.utc(2020, 1, 1))
        col.max(remove_na: true).should eq(Time.utc(2023, 6, 15))
      end

      it "compares with Time values" do
        times = [Time.utc(2020, 1, 1), Time.utc(2023, 6, 15), Time.utc(2025, 12, 31)]
        col = TimestampCol.new("ts", times)
        threshold = Time.utc(2023, 1, 1)
        (col > threshold).should eq([false, true, true])
        (col >= threshold).should eq([false, true, true])
        (col < threshold).should eq([true, false, false])
        (col <= threshold).should eq([true, false, false])
      end

      it "supports lazy iteration" do
        times = [Time.utc(2023, 1, 1), nil, Time.utc(2023, 12, 31)]
        col = TimestampCol.new("ts", times)
        collected = [] of Time?
        col.each { |v| collected << v }
        collected.size.should eq(3)
        collected[1].should be_nil
        non_null = [] of Time
        col.each_non_null { |v| non_null << v }
        non_null.size.should eq(2)
      end

      it "creates from epoch nanoseconds" do
        epochs = [1672531200123456789_i128, nil]
        col = TimestampCol.from_epoch_ns("ts", epochs)
        col[0].not_nil!.year.should eq(2023)
        col[0].not_nil!.nanosecond.should eq(123456789)
        col[1].should be_nil
      end

      it "formats with strftime" do
        times = [Time.utc(2023, 1, 15, 10, 30, 45, nanosecond: 123_456_789)]
        col = TimestampCol.new("ts", times)
        formatted = col.strftime("%Y-%m-%d %H:%M:%S.%N")
        formatted[0].should eq("2023-01-15 10:30:45.123456789")
      end

      it "converts to DateTimeCol" do
        times = [Time.utc(2023, 1, 15, 10, 30, 45, nanosecond: 123_456_789)]
        ts = TimestampCol.new("ts", times)
        dt = ts.to_datetime
        dt.is_a?(DateTimeCol).should be_true
        dt[0].not_nil!.year.should eq(2023)
        dt[0].not_nil!.second.should eq(45)
      end

      it "supports lead/lag" do
        times = [Time.utc(2023, 1, 1), Time.utc(2023, 1, 2), Time.utc(2023, 1, 3)]
        col = TimestampCol.new("ts", times)
        col.lead(1)[0].should eq(Time.utc(2023, 1, 2))
        col.lead(1)[2].should be_nil
        col.lag(1)[0].should be_nil
        col.lag(1)[1].should eq(Time.utc(2023, 1, 1))
      end

      it "supports ffill/bfill" do
        times = [Time.utc(2023, 1, 1), nil, Time.utc(2023, 1, 3)]
        col = TimestampCol.new("ts", times)
        col.ffill[0].should eq(Time.utc(2023, 1, 1))
        col.ffill[1].should eq(Time.utc(2023, 1, 1))
        col.ffill[2].should eq(Time.utc(2023, 1, 3))
        col.bfill[0].should eq(Time.utc(2023, 1, 1))
        col.bfill[1].should eq(Time.utc(2023, 1, 3))
        col.bfill[2].should eq(Time.utc(2023, 1, 3))
      end
    end

    describe "DateTimeCol#to_timestamp" do
      it "converts to TimestampCol" do
        times = [Time.utc(2023, 1, 15, 10, 30, 45, nanosecond: 123_000_000)]
        dt = DateTimeCol.new("dt", times)
        ts = dt.to_timestamp
        ts.is_a?(TimestampCol).should be_true
        ts[0].not_nil!.year.should eq(2023)
        ts[0].not_nil!.nanosecond.should eq(123000000)
      end
    end

    describe "BigDecimalCol" do
      it "stores and retrieves values" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("10.5"), nil, BigDecimal.new("3.14")])
        col[0].should eq(BigDecimal.new("10.5"))
        col[1].should be_nil
        col[2].should eq(BigDecimal.new("3.14"))
      end

      it "computes sum" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("1"), BigDecimal.new("2"), BigDecimal.new("3")])
        col.sum.should eq(BigDecimal.new("6"))
      end

      it "computes mean" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("2"), BigDecimal.new("4"), BigDecimal.new("6")])
        col.mean.should eq(BigDecimal.new("4"))
      end

      it "computes min and max" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("3"), BigDecimal.new("1"), BigDecimal.new("2")])
        col.min.should eq(BigDecimal.new("1"))
        col.max.should eq(BigDecimal.new("3"))
      end

      it "forward fills nulls" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("1.5"), nil, BigDecimal.new("3.0")])
        result = col.ffill
        result[0].should eq(BigDecimal.new("1.5"))
        result[1].should eq(BigDecimal.new("1.5"))
        result[2].should eq(BigDecimal.new("3.0"))
      end

      it "backward fills nulls" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("1.5"), nil, BigDecimal.new("3.0")])
        result = col.bfill
        result[0].should eq(BigDecimal.new("1.5"))
        result[1].should eq(BigDecimal.new("3.0"))
        result[2].should eq(BigDecimal.new("3.0"))
      end

      it "supports arithmetic" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("10"), BigDecimal.new("20")])
        (col + 5)[0].should eq(BigDecimal.new("15"))
        (col - 3)[0].should eq(BigDecimal.new("7"))
        (col * 2)[0].should eq(BigDecimal.new("20"))
        (col / 2)[0].should eq(BigDecimal.new("5"))
      end

      it "works in a DataFrame" do
        df = Crysda.dataframe_of(
          BigDecimalCol.new("val", [BigDecimal.new("1.1"), BigDecimal.new("2.2"), BigDecimal.new("3.3")]),
          StringCol.new("label", ["a", "b", "c"]),
        )
        df.num_row.should eq(3)
        df.num_col.should eq(2)
        df["val"].is_a?(BigDecimalCol).should be_true
        df["val"].as(BigDecimalCol).sum.should eq(BigDecimal.new("6.6"))
      end

      it "detects BigDecimal arrays via handle_union" do
        arr = [BigDecimal.new("1.5"), BigDecimal.new("2.5"), nil]
        col = Utils.handle_union("test", arr)
        col.is_a?(BigDecimalCol).should be_true
        col[0].should eq(BigDecimal.new("1.5"))
        col[2].should be_nil
      end

      it "converts to f64" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("1.5"), BigDecimal.new("2.5")])
        result = col.as_f64
        result.should eq([1.5, 2.5])
      end

      it "converts to_s" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("1.5"), nil, BigDecimal.new("3.0")])
        result = col.as_s
        result[0].should eq("1.5")
        result[1].should be_nil
        result[2].should eq("3.0")
      end

      it "supports filter operations" do
        df = Crysda.dataframe_of(
          BigDecimalCol.new("bd", [BigDecimal.new("1"), BigDecimal.new("5"), BigDecimal.new("3")]),
        )
        filtered = df.filter { |r| r["bd"] > 2 }
        filtered.num_row.should eq(2)
      end

      it "supports comparison" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("1"), BigDecimal.new("5"), BigDecimal.new("3")])
        result = col > 2
        result.should eq([false, true, true])
      end

      it "computes median" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("3"), BigDecimal.new("1"), BigDecimal.new("2")])
        col.median.should eq(BigDecimal.new("2"))
      end

      it "converts to Float64Col" do
        col = BigDecimalCol.new("bd", [BigDecimal.new("1.5"), BigDecimal.new("2.5"), nil])
        f64 = col.to_f64
        f64.is_a?(Float64Col).should be_true
        f64[0].should eq(1.5)
        f64[1].should eq(2.5)
        f64[2].should be_nil
      end

      it "converts from Float64Col to BigDecimalCol" do
        col = Float64Col.new("f64", [1.5, 2.5, nil])
        bd = col.to_big_decimal
        bd.is_a?(BigDecimalCol).should be_true
        bd[0].should eq(BigDecimal.new("1.5"))
        bd[1].should eq(BigDecimal.new("2.5"))
        bd[2].should be_nil
      end
    end
  end
end
