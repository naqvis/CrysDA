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
  end
end
