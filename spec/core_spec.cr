require "./spec_helper"

module Crysda
  class Something < CustomColumnValue
    def to_s
      "Something#{hash}"
    end

    def to_s(io : IO) : Nil
      io << to_s
    end

    def hashcode : Int64
      17_i64
    end
  end

  describe "Select" do
    it "allow for empty data frame" do
      DataFrame.empty
      column_types(DataFrame.empty)
      DataFrame.empty.head
      DataFrame.empty.tail
      DataFrame.empty.select(Int32Col)
    end

    it "should select with regex" do
      SLEEP_DATA.select(&.ends_with?("wt")).num_col.should eq(2)
      SLEEP_DATA.select(&.starts_with?("sleep")).num_col.should eq(3)
      SLEEP_DATA.select(&.list_of("conservation", "foobar", "order")).num_col.should eq(2)

      SLEEP_DATA.select(Int32Col)
      SLEEP_DATA.select?(&.is_a?(Int32Col))
      SLEEP_DATA.select?(&.name.starts_with?("foo"))

      IRIS_DATA.select(StringCol).names.should eq(["Species"])
    end

    it "should allow to remove columns" do
      SLEEP_DATA.reject(&.ends_with?("wt")).num_col.should eq(9)
      SLEEP_DATA.reject(&.starts_with?("sleep")).num_col.should eq(8)
      SLEEP_DATA.reject(&.list_of("conservation", "foobar", "order")).num_col.should eq(9)

      IRIS_DATA.reject(StringCol).num_col.should eq(4)
      IRIS_DATA.reject?(&.is_a?(StringCol)).num_col.should eq(4)
      IRIS_DATA.reject?(&.name.starts_with?("Sepal")).num_col.should eq(3)

      # also allow for negative selection (like in the context of gather)
      IRIS_DATA.select(&.except(&.starts_with?("Sepal"))).num_col.should eq(3)
    end

    it "should not allow to select non-existing column" do
      expect_raises(SelectException) do
        SLEEP_DATA.select("foobar")
      end
    end

    it "should  allow to select no column" do
      SLEEP_DATA.select([] of String).num_col.should eq(0)
      IRIS_DATA.select(&.starts_with?("bla")).num_col.should eq(0)
    end

    it "should not allow to select columns twice" do
      expect_raises(SelectException, "Columns must not be selected more than once") do
        SLEEP_DATA.select("name", "vore", "name")
      end
      SLEEP_DATA.select("name", "vore").num_col.should eq(2)
    end

    it "should do a negative selection" do
      SLEEP_DATA.reject("name", "vore").tap do |df|
        df.names.includes?("name").should eq(false)
        df.names.includes?("vore").should eq(false)

        # ensure preserved order of remaining columns
        (SLEEP_DATA.names - ["name", "vore"]).should eq(df.names)
      end

      IRIS_DATA.select(&.starts_with?("Sepal").not).names.should eq(["Petal.Length", "Petal.Width", "Species"])
    end

    it "it should not allow a mixed negative and positive selection" do
      # note: typically the user would perform a positive selection but in context like gather he needs a negative selection api as well
      column_types(IRIS_DATA.select { |e| e.except("Species").and e.starts_with?("Sepal").not }).size.should eq(2)
      column_types(IRIS_DATA.select { |e| e.except("Species").and e.except(&.starts_with?("Sepal")) }).size.should eq(2)

      # but one must never mix positive and negative selection
      expect_raises(InvalidColumnSelectException, "Mixing positive and negative selection does not have meaningful semantics and is not supported") do
        IRIS_DATA.select { |e| e.except("Species").and e.starts_with?("Sepal") }
      end
    end

    it "should handle empty negative selections gracefully" do
      IRIS_DATA.select(&.except(""))
    end

    it "should allow to select with matchers in grouped df" do
      IRIS_DATA.group_by("Species")
        .select(&.ends_with?("Length"))
        .tap do |df|
          df.names.should eq(["Species", "Sepal.Length", "Petal.Length"])
        end
    end
  end

  describe "Columns" do
    it "rename columns and preserve their positions" do
      SLEEP_DATA.rename({name: "vore", with: "new_vore"}, {name: "awake", with: "awa2"})
        .tap do |df|
          df.names.includes?("vore").should eq(false)
          df.names.includes?("new_vore").should eq(true)

          # column renaming should preserve positions
          df.names.index("new_vore").should eq(SLEEP_DATA.names.index("vore"))

          # renaming should not affect column or row counts
          df.num_row.should eq(SLEEP_DATA.num_row)
          df.num_col.should eq(SLEEP_DATA.num_col)
        end
    end

    it "it should allow dummy rename" do
      SLEEP_DATA.rename({name: "vore", with: "vore"}).names.should eq(SLEEP_DATA.names)
    end

    it "it should  mutate existing columns while keeping their position" do
      IRIS_DATA.add_column("Sepal.Length".with { |e| e["Sepal.Length"] + 10 }).names.should eq(IRIS_DATA.names)
    end

    it "it should  allow to use a new column in the same mutate call" do
      SLEEP_DATA.add_columns(
        "vore_new".with { |e| e["vore"] },
        "vore_first_char".with { |e| e["vore"].map(&.to_s[0].to_s) }
      )
    end

    it "it should  allow add a rownumber column" do
      SLEEP_DATA.add_column("user_id") { |e| e.const("id") + e.row_num }["user_id"][1].should eq("id2")

      # again but with explicit type convertion
      SLEEP_DATA.add_column("user_id") { |e| e.const("id").as_s.zip(e.row_num).map { |l, r| l.not_nil! + r.to_s } }["user_id"][1].should eq("id2")

      SLEEP_DATA.add_row_number.names.first.should eq("row_number")
    end

    it "it should gracefully reject incorrect type casts" do
      expect_raises(Exception) do
        SLEEP_DATA.add_column("foo", &.["vore"].as_i)
      end
    end

    it "it should allow to create columns from Any scalars" do
      obj = UUID.random
      dataframe_of("foo").values("bar").add_column("some_uuid") { obj }.tap do |df|
        df.names.should eq(["foo", "some_uuid"])
        df[1][0].should eq(obj)
      end
    end

    it "it should perform correct column arithmetics" do
      df = dataframe_of("product", "weight", "price", "num_items", "tax", "inflammable").values(
        "handy", 2.0, 1.0, 33, 10i64, true,
        "tablet", 1.5, 6.0, 22, 5i64, true,
        "macbook", 12.5, 20.0, 4, 2i64, false
      )

      df.add_column("price_per_kg") { |e| e["price"] / e["weight"] }["price_per_kg"].as_f64.should eq([0.5, 4.0, 1.6])
      df.add_column("value") { |e| e["num_items"] * e["price"] }["value"].as_f64.should eq([33.0, 132.0, 80.0])

      # same but with reversed arguments
      df.add_column("value") { |e| e["price"] * e["num_items"] }["value"].as_f64.should eq([33.0, 132.0, 80.0])
    end
  end

  describe "Filter" do
    it "head tail and slice should extract data as expected" do
      SLEEP_DATA.take.num_row.should eq(5)
      SLEEP_DATA.take_last(5).num_row.should eq(5)
      SLEEP_DATA.slice(1, 3, 5).num_row.should eq(3)
      SLEEP_DATA.slice(3..5).num_row.should eq(3)
    end

    it "should filter in empty table" do
      SLEEP_DATA
        .filter { |e| e["name"] == "foo" }
        # refilter on empty one
        .filter { |e| e.["name"] == "bar" }
    end

    it "should sub sample data" do
      # fixed sampling should work
      SLEEP_DATA.sample_n(2).num_row.should eq(2)
      # oversampling
      SLEEP_DATA.sample_n(1000, replace: true).num_row.should eq(1000)

      # fractional sampling should work as well
      SLEEP_DATA.sample_frac(0.3).num_row.should eq((SLEEP_DATA.num_row * 0.3).round.to_i)
      SLEEP_DATA.sample_frac(0.3, true).num_row.should eq((SLEEP_DATA.num_row * 0.3).round.to_i)
      SLEEP_DATA.sample_frac(2.0, true).num_row.should eq(SLEEP_DATA.num_row * 2)

      # test boundary conditions
      SLEEP_DATA.sample_n(0).num_row.should eq(0)
      SLEEP_DATA.sample_n(0, true).num_row.should eq(0)
      SLEEP_DATA.sample_frac(0.0).num_row.should eq(0)
      SLEEP_DATA.sample_frac(0.0, true).num_row.should eq(0)

      SLEEP_DATA.sample_n(SLEEP_DATA.num_row).num_row.should eq(SLEEP_DATA.num_row)
      SLEEP_DATA.sample_n(SLEEP_DATA.num_row, true).num_row.should eq(SLEEP_DATA.num_row)
      SLEEP_DATA.sample_frac(1.0).num_row.should eq(SLEEP_DATA.num_row)
      SLEEP_DATA.sample_frac(1.0, true).num_row.should eq(SLEEP_DATA.num_row)

      # make sure that invalid sampling parameters raises exceptions

      expect_raises(Exception) do
        SLEEP_DATA.sample_n(-1)
        SLEEP_DATA.sample_n(-1, true)
        SLEEP_DATA.sample_frac(-0.3)
        SLEEP_DATA.sample_frac(-0.3, true)
      end

      # oversampling without replacement should not work
      expect_raises(Exception, "can not over-sample data without replace") do
        SLEEP_DATA.sample_n(1000)
        SLEEP_DATA.sample_frac(1.3)
      end

      # fixed sampling of grouped data should be done per group
      group_counts = SLEEP_DATA.group_by("vore").sample_n(2).count("vore")
      group_counts["n"].as_i.uniq.tap do |gd|
        gd.size.should eq(1)
        gd.first.should eq(2)
      end

      # fractional sampling of grouped data should be done per group
      SLEEP_DATA
        .group_by("vore")
        .sample_frac(0.5)
        .count("vore")
        .filter { |e| e["vore"] == "omni" }
        .tap(&.["n"].as_i.first.should eq(10))
    end

    it "should filter rows with text matching helpers" do
      SLEEP_DATA.filter { |e| e["vore"].matching(&.== "insecti") }.num_row.should eq(5)
      SLEEP_DATA.filter { |e| e["vore"].matching(&.starts_with?("ins")) }.num_row.should eq(5)

      df = dataframe_of("x").values(1, 2, 3, 4, 5, nil)
      df.filter { |e| e["x"] > 2 }.tap do |fi|
        fi.filter(&.is_na("x")).num_row.should eq(0)
        fi.num_row.should eq(3)
      end

      df.filter { |a| a["x"] >= 2 }.num_row.should eq(4)
      df.filter { |a| a["x"] < 2.0 }.num_row.should eq(1)
      df.filter { |a| a["x"] <= 2i64 }.num_row.should eq(2)
    end

    it "should allow for vectorized filter expressions" do
      IRIS_DATA.filter { |e| (e["Sepal.Length"] > e["Petal.Length"] * 3).and(e["Species"] == "setosa") }.num_row.should eq(44)
    end
  end

  describe "Sort" do
    data = dataframe_of("user_id", "name").values(
      6, "maja",
      3, "anna",
      nil, "max",
      5, nil,
      1, "tom",
      5, "tom"
    )

    it "order and rank should behave properly" do
      # rank returns the order of each element in an ascending list
      # order returns the index each element would have in an ascending list

      y = Float64Col.new("foo", [3.5, 3.0, 3.2, 3.1, 3.6, 3.9, 3.4, 3.4, 2.9, 3.1])
      y.rank.should eq([7, 1, 4, 2, 8, 9, 5, 6, 0, 3])
      y.order.should eq([8, 1, 3, 9, 2, 6, 7, 0, 4, 5])
    end

    it "sort numeric columns" do
      data.sort_by("user_id")["user_id"]
        .tap do |df|
          df.values[0].should eq(1)
          df.as_i.should eq([1, 3, 5, 5, 6, nil])
        end
    end

    it "sort numeric columns in descending order" do
      data.sort_desc_by("user_id")
        .tap do |df|
          df["user_id"][0].should eq(6)
          df["name"][0].should eq("maja")
          df["user_id"][5].should eq(nil)
          df["name"][5].should eq("max")
        end

      # also checking order if NA's are present in data (they should come last)
      SLEEP_DATA.sort_desc_by("sleep_rem")["sleep_rem"][0].should eq(6.6)
    end

    it "resolve ties if needed" do
      # test would require a tie-resolve if sleep_rem would be included as second sorting attribute
      SLEEP_DATA.sort_by("order", "sleep_total")["sleep_total"].as_f64[1].should eq(1.9)

      # also mix asc and desc sorting
      SLEEP_DATA.sort_by([SortExpression.new { |e| e["order"] },
                          SortExpression.new { |e| e.desc(e["sleep_total"]) },
      ])["sleep_total"].as_f64[1].should eq(9.1)
    end

    it "should fail for invalid sorting predicates" do
      expect_raises(InvalidSortingPredicateException) do
        SLEEP_DATA.sort_by { |_| "order" }
      end
    end
  end

  describe "Summarize" do
    it "should fail if summaries are not scalar values" do
      expect_raises(NonScalarValueException) do
        SLEEP_DATA.summarize("foo") { |_| ["a", "b", "c"] }
        SLEEP_DATA.summarize("foo") { |_| Array(Bool).new(12, false) }
      end
    end

    it "should allow complex objects as summaries" do
      # Due to language restrictions of not allowing Top-level Referece/Object
      # as a marker. One need to inherit from `CustomColumnValue` class
      SLEEP_DATA.group_by("vore").summarize(
        "foo".with { |_| Something.new },
        "bar".with { |_| Something.new }
      ).print
    end

    it "count should behave like dplyr-count" do
      IRIS_DATA.count.should eq(dataframe_of("n").values(150))

      # prevent duplicated column names
      expect_raises(DuplicateColumnNameException, "'n' is already present in data-frame") do
        IRIS_DATA.count.count("n")
      end

      IRIS_DATA.count.count("n", name: "new_n").names.should eq(["n", "new_n"])

      # is an existing group preserved
      IRIS_DATA.group_by("Species").count.num_row.should eq(3)
    end

    it "count should work with function literals" do
      SLEEP_DATA.add_columns("sleep_na".with(&.["sleep_rem"].is_na)).count("sleep_na")

      # should be equivalent to
      SLEEP_DATA.group_by_expr(TableExpression.new(&.["sleep_rem"].is_na)).count.print
      SLEEP_DATA.group_by_expr(
        TableExpression.new(&.["sleep_rem"].is_na),
        TableExpression.new(&.["sleep_rem"].is_na),
      ).count.print
      SLEEP_DATA.group_by_expr.count.print
    end

    it "summarize multiple columns at once with summarize_at" do
      IRIS_DATA.summarize_at(
        ColumnSelector.new(&.starts_with?("Sepal")),
        SummarizeFunc.new do |s|
          s.add(SumFormula.new(&.mean), "mean")
          s.add(SumFormula.new(&.median), "median")
        end
      ).tap do |df|
        df.print
        df.num_row.should eq(1)
        df.names.size.should eq(4)
      end

      # using variadic arguments
      IRIS_DATA.summarize_at(
        ColumnSelector.new(&.ends_with?("Length")),
        AggFuncs.mean,
        # AggFuncs.median,
        AggFunc.new(SumFormula.new(&.median), "median")
      ).tap do |df|
        df.print
        df.num_row.should eq(1)
        df.names.size.should eq(4)
      end
    end

    it "summarize multiple columns in grouped data frame with summarize_at" do
      IRIS_DATA.group_by("Species")
        .summarize_at(
          ColumnSelector.new(&.ends_with?("Length")),
          AggFuncs.mean
        ).tap do |df|
        df.print
        df.num_row.should eq(3)
        df.names.should eq(["Species", "Sepal.Length.mean", "Petal.Length.mean"])
      end
    end
  end

  describe "Core" do
    it "should handle empty (row and column-empty) data-frames in all operations" do
      empty_df.tap do |df|
        df.num_col.should eq(0)
        df.num_row.should eq(0)
        df.rows.size.should eq(0)
        df.cols.size.should eq(0)

        # rendering
        df.schema
        df.print

        df.select([] of String)
        # core verbs
        df.filter { |_| [] of Bool }
        df.add_column("foo") { |_| "bar" }
        df.summarize("foo".with { |_| "bar" })
        df.sort_by

        # grouping
        (df.group_by).grouped_by
      end
    end

    it "should round numbers when printing" do
      df = dataframe_of("a").values(Random.new(3).rand, nil)
      output = <<-OUT
      A DataFrame: 2 x 1
                a
      1   0.08003
      2      <NA>
      OUT
      df.to_string(max_digits: 5).should eq(output)
    end

    it "should print schemas with correct alignment and truncation" do
      iris2 = IRIS_DATA.add_column("id") { |e| e.row_num.map { |f| "foo#{f}" } }
      io = IO::Memory.new
      iris2.schema(max_digits: 1, max_width: 20, output: io)
      output = <<-OUT
      DataFrame with 150 observations
      Sepal.Length [Float64] 5.1, 4.9, 4.7, 4.6, ...
      Sepal.Width  [Float64] 3.5, 3.0, 3.2, 3.1, ...
      Petal.Length [Float64] 1.4, 1.4, 1.3, 1.5, ...
      Petal.Width  [Float64] 0.2, 0.2, 0.2, 0.2, ...
      Species      [String]  setosa, setosa, seto...
      id           [String]  foo1, foo2, foo3, fo...

      OUT

      io.to_s.should eq(output)
    end

    it "should allow to peek into columns" do
      IRIS_DATA["Sepal.Length"].to_s.should eq "Sepal.Length [Float64][150]: 5.1, 4.9, 4.7, 4.6, 5.0, 5.4, 4.6, 5.0, 4.4, 4.9, 5.4, 4.8, 4.8, 4.3, ..."
      IRIS_DATA["Species"].to_s.should eq "Species [String][150]: setosa, setosa, setosa, setosa, setosa, setosa, setosa, setosa, setosa, ..."
    end

    it "should print just first columns and rows" do
      output = <<-OUT
A DataFrame: 83 x 11
                           name         genus    vore          order   conservation   sleep_total
 1                      Cheetah      Acinonyx   carni      Carnivora             lc        12.100
 2                   Owl monkey         Aotus    omni       Primates           <NA>        17.000
 3              Mountain beaver    Aplodontia   herbi       Rodentia             nt        14.400
 4   Greater short-tailed shrew       Blarina    omni   Soricomorpha             lc        14.900
 5                          Cow           Bos   herbi   Artiodactyla   domesticated         4.000
 6             Three-toed sloth      Bradypus   herbi         Pilosa           <NA>        14.400
 7            Northern fur seal   Callorhinus   carni      Carnivora             vu         8.700
 8                 Vesper mouse       Calomys    <NA>       Rodentia           <NA>         7.000
 9                          Dog         Canis   carni      Carnivora   domesticated        10.100
10                     Roe deer     Capreolus   herbi   Artiodactyla             lc         3.000
and 73 more rows, and 5 more variables: sleep_cycle, awake, brainwt, bodywt
OUT
      SLEEP_DATA.to_string.should eq(output)
    end

    it "should print an empty dataframe as such" do
      io = IO::Memory.new
      empty_df.print(output: io)
      io.to_s.should eq("A DataFrame: 0 x 0\n")
      io.clear
      output = <<-STR
A DataFrame: 0 x 5
        Sepal.Length            Sepal.Width           Petal.Length            Petal.Width
1 more variables: Species

STR

      IRIS_DATA.filter { |f| f["Species"] == "foo" }.print(output: io)
      io.to_s.should eq(output)
    end
  end

  describe "Group" do
    it "should allow for NA as a group value" do
      # 1. test single attribute grouping with NA
      SLEEP_DATA.group_by("vore").grouped_by.num_row.should eq(5)
      # 2. test multi-attribute grouping with NA in one or all attributes
      # todo
    end

    it "distince avoids hashcode collision" do
      df = dataframe_of("a", "b", "c").values(
        3, 263, 5,
        3, 325, 6,
        5, 201, 1,
        5, 263, 2,
        5, 265, 3,
        5, 325, 4
      )

      df.rows.to_a.should eq(df.distinct("a", "b").rows.to_a)
    end

    it "should count group sizes and report distinct rows in a table" do
      SLEEP_DATA.count("vore").tap do |df|
        df.print
        df.num_col.should eq 2
        df.num_row.should eq 5
      end

      SLEEP_DATA.distinct("vore", "order").tap do |df|
        df.print
        df.num_row.should eq 32
        df.num_col.should eq 11
      end
    end

    it "should calculate same group hash irrespective of column order" do
      df = dataframe_of("first_name", "last_name", "age", "weight").values(
        "Max", "Doe", 23, 55,
        "Franz", "Smith", 23, 88,
        "Horst", "Keanes", 12, 82,
      )

      dfb = df.select("age", "last_name", "weight", "first_name")

      # by joining with multiple attributes we inherentily group (which is the actual test
      df.left_join(dfb, by: ["last_name", "first_name"]).tap(&.num_row.should eq(3))
    end

    it "it should group tables with object columns and by object column" do
      u1 = UUID.random
      u2 = UUID.random
      df = dataframe_of("id", "quantity").values(
        u1, 1,
        u2, 1,
        u2, 2,
      )

      # first group by primitive column
      df.group_by("quantity").tap do |f|
        f.print
        f.groups.size.should eq 2
      end

      # second group by object column itself
      df.group_by("id").tap do |f|
        f.print
        f.groups.size.should eq 2
      end
    end

    it "should preserve column shape when grouping data-frame without rows" do
      df = dataframe_of(StringCol.new("foo", [] of String), Int32Col.new("bar", [] of Int32))
      df.print
      df.group_by("foo").tap do |f|
        f.names.should eq(["foo", "bar"])
      end
    end
  end

  describe "Bind Rows" do
    it "should add complete rows" do
      df = dataframe_of("person", "year", "weight", "sex").values(
        "max", 2014, 33.1, "M",
        "max", 2016, nil, "M",
        "anna", 2015, 39.2, "F",
        "anna", 2016, 39.9, "F"
      )

      row1 = {
        "person" => "james",
        "year"   => 1996,
        "weight" => 54.0,
        "sex"    => "M",
      } of String => Any

      row2 = {
        "person" => "nell",
        "year"   => 1997,
        "weight" => 48.1,
        "sex"    => "F",
      } of String => Any

      df.bind_rows(row1, row2).tap do |f|
        f.print
        f.num_row.should eq(6)
        f.num_col.should eq(4)
        rows = f.rows.to_a
        rows[1]["weight"].as_nil.should eq(nil)
        rows[4]["person"].as_s.should eq("james")
        rows[4]["weight"].as_f.should eq(54.0)
        rows[5]["person"].as_s.should eq("nell")
        rows[5]["year"].as_i.should eq(1997)
      end

      # Check that the original has not been modified
      df.num_row.should eq(4)
    end

    it "should insert NaN for missing columns" do
      df = dataframe_of("person", "year", "weight", "sex").values(
        "max", 2014, 33.1, "M",
        "max", 2016, nil, "M",
        "anna", 2015, 39.2, "F",
        "anna", 2016, 39.9, "F"
      )

      row = {
        "person" => "james",
        "year"   => 1996,
      } of String => Any

      df.bind_rows(row).tap do |f|
        f.num_row.should eq(5)
        f.num_col.should eq(4)
        rows = f.rows.to_a
        rows[1]["weight"].as_nil.should eq(nil)
        rows[4]["person"].as_s.should eq("james")
        rows[4]["weight"].as_nil.should eq(nil)
        rows[4]["sex"].as_nil.should eq(nil)
      end

      # Check that the original has not been modified
      df.num_row.should eq(4)
    end

    it "should create new columns as needed" do
      df = dataframe_of("person", "year", "weight", "sex").values(
        "max", 2014, 33.1, "M",
        "max", 2016, nil, "M",
        "anna", 2016, 39.9, "F"
      )

      row = {
        "person"  => "batman",
        "nemesis" => "joker",
      } of String => Any

      df.bind_rows(row).tap do |f|
        f.num_col.should eq(5)
        f.num_row.should eq(4)
      end
    end
  end

  describe "Compound" do
    it "should summarize sleep data" do
      SLEEP_DATA
        .filter { |f| f["awake"] > 3 }
        .tap(&.schema)
        .add_column("rem_proportion") { |c| c["sleep_rem"] + c["sleep_rem"] }
        .group_by("vore")
        .tap(&.print)

      mean_rem_prop_insecti = SLEEP_DATA
        .filter { |f| f["awake"] > 3 }
        .add_column("rem_proportion") { |c| c["sleep_rem"] / c["sleep_total"] }
        .move_left("rem_proportion", "sleep_rem", "sleep_total")
        .group_by("vore")
        .tap(&.print)
        .summarize("mean_rem_prop") { |s| s["rem_proportion"].mean(remove_na: true) }
        .tap(&.print)
        .filter { |f| f["vore"] == "insecti" }.row(0)["mean_rem_prop"].as_f

      mean_rem_prop_insecti.should eq(0.22137215757391437)
    end

    it "should allow to create dataframe in place" do
      df = dataframe_of("foo", "bar").values(
        "ll", 2,
        "sdfd", 4,
        "sdf", 5
      )

      df.num_col.should eq(2)
      df.num_row.should eq(3)
      df.names.should eq(["foo", "bar"])

      na_df = dataframe_of("foo", "bar").values(
        nil, nil,
        "sdfd", nil,
        "sdf", 5
      )
      na_df["foo"].is_a?(StringCol).should be_true
      na_df["bar"].is_a?(Int32Col).should be_true

      na_df.summarize("num_na") { |f| f["bar"].is_na.sum { |v| v ? 1 : 0 } }.print
    end
  end
end
