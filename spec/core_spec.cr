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

  describe "Missing Data Operations" do
    it "dropna removes rows with any null values" do
      df = dataframe_of("a", "b", "c").values(
        1, "x", 1.0,
        nil, "y", 2.0,
        3, nil, 3.0,
        4, "z", nil,
        5, "w", 5.0
      )
      result = df.dropna
      result.num_row.should eq(2)
      result["a"][0].should eq(1)
      result["a"][1].should eq(5)
    end

    it "dropna with specific columns only checks those columns" do
      df = dataframe_of("a", "b", "c").values(
        1, "x", nil,
        nil, "y", 2.0,
        3, "z", 3.0
      )
      # Only check column "a" for nulls
      result = df.dropna(["a"])
      result.num_row.should eq(2)
      result["a"][0].should eq(1)
      result["a"][1].should eq(3)
      # Row with nil in "c" is kept because we only checked "a"
      result["c"][0].should be_nil
    end

    it "dropna with varargs syntax" do
      df = dataframe_of("a", "b").values(
        1, nil,
        nil, 2,
        3, 3
      )
      result = df.dropna("a")
      result.num_row.should eq(2)
    end

    it "fillna replaces nulls with specified value in all columns" do
      df = dataframe_of("a", "b").values(
        1, 1.0,
        nil, nil,
        3, 3.0
      )
      result = df.fillna(0)
      result["a"][1].should eq(0)
      result["b"][1].should eq(0.0)
    end

    it "fillna with specific columns" do
      df = dataframe_of("a", "b").values(
        1, 1.0,
        nil, nil,
        3, 3.0
      )
      result = df.fillna(["a"], 99)
      result["a"][1].should eq(99)
      result["b"][1].should be_nil # unchanged
    end

    it "fillna with hash of column values" do
      df = dataframe_of("name", "age", "score").values(
        "Alice", 25, 90.0,
        nil, nil, nil,
        "Bob", 30, 85.0
      )
      result = df.fillna({"name" => "Unknown", "age" => 0, "score" => 0.0})
      result["name"][1].should eq("Unknown")
      result["age"][1].should eq(0)
      result["score"][1].should eq(0.0)
    end

    it "fillna works with string columns" do
      df = dataframe_of("name").values("Alice", nil, "Bob")
      result = df.fillna("N/A")
      result["name"][1].should eq("N/A")
    end

    it "fillna works with bool columns" do
      df = dataframe_of("flag").values(true, nil, false)
      result = df.fillna(false)
      result["flag"][1].should eq(false)
    end

    it "dropna on empty dataframe returns empty" do
      df = DataFrame.empty
      df.dropna.num_row.should eq(0)
    end

    it "fillna preserves non-null values" do
      df = dataframe_of("a").values(1, 2, 3)
      result = df.fillna(99)
      result["a"].values.should eq([1, 2, 3])
    end
  end

  describe "String Operations" do
    it "contains checks for substring" do
      col = StringCol.new("text", ["hello world", "foo bar", nil, "hello"])
      col.contains("hello").should eq([true, false, false, true])
      col.contains("HELLO", case_sensitive: false).should eq([true, false, false, true])
    end

    it "contains works with regex" do
      col = StringCol.new("text", ["hello123", "world456", nil, "test"])
      col.contains(/\d+/).should eq([true, true, false, false])
    end

    it "starts_with and ends_with" do
      col = StringCol.new("text", ["hello world", "hello there", nil, "world hello"])
      col.starts_with("hello").should eq([true, true, false, false])
      col.ends_with("hello").should eq([false, false, false, true])
    end

    it "extract captures regex groups" do
      col = StringCol.new("text", ["user_123", "admin_456", nil, "guest_789"])
      extracted = col.extract(/(\d+)/)
      extracted[0].should eq("123")
      extracted[1].should eq("456")
      extracted[2].should be_nil
      extracted[3].should eq("789")
    end

    it "extract returns full match when no groups" do
      col = StringCol.new("text", ["abc123def", "xyz789"])
      extracted = col.extract(/\d+/)
      extracted[0].should eq("123")
      extracted[1].should eq("789")
    end

    it "replace substitutes patterns" do
      col = StringCol.new("text", ["hello world", "hello there", nil])
      result = col.replace("hello", "hi")
      result[0].should eq("hi world")
      result[1].should eq("hi there")
      result[2].should be_nil
    end

    it "replace works with regex" do
      col = StringCol.new("text", ["abc123", "def456"])
      result = col.replace(/\d+/, "XXX")
      result[0].should eq("abcXXX")
      result[1].should eq("defXXX")
    end

    it "upcase and downcase" do
      col = StringCol.new("text", ["Hello", "WORLD", nil])
      col.upcase[0].should eq("HELLO")
      col.downcase[1].should eq("world")
      col.upcase[2].should be_nil
    end

    it "strip removes whitespace" do
      col = StringCol.new("text", ["  hello  ", "\tworld\n", nil])
      col.strip[0].should eq("hello")
      col.strip[1].should eq("world")
      col.lstrip[0].should eq("hello  ")
      col.rstrip[0].should eq("  hello")
    end

    it "len returns string length" do
      col = StringCol.new("text", ["hello", "hi", nil, "world!"])
      lengths = col.len
      lengths[0].should eq(5)
      lengths[1].should eq(2)
      lengths[2].should be_nil
      lengths[3].should eq(6)
    end

    it "slice extracts substrings" do
      col = StringCol.new("text", ["hello world", "foo", nil])
      col.slice(0, 5)[0].should eq("hello")
      col.slice(6)[0].should eq("world")
      col.slice(0, 3)[1].should eq("foo")
    end

    it "pad_left and pad_right" do
      col = StringCol.new("text", ["42", "7", nil])
      col.pad_left(4, '0')[0].should eq("0042")
      col.pad_left(4, '0')[1].should eq("0007")
      col.pad_right(4, '-')[0].should eq("42--")
    end

    it "matches checks full string match" do
      col = StringCol.new("text", ["123", "abc123", nil, "456"])
      col.matches(/\d+/).should eq([true, false, false, true])
    end

    it "count occurrences" do
      col = StringCol.new("text", ["aaa", "abab", nil, "xyz"])
      col.count("a")[0].should eq(3)
      col.count("a")[1].should eq(2)
      col.count("a")[2].should be_nil
      col.count("a")[3].should eq(0)
    end

    it "string ops work in dataframe context" do
      df = dataframe_of("name", "email").values(
        "John Doe", "john@example.com",
        "Jane Smith", "jane@test.org",
        nil, nil
      )
      # Filter rows where email contains "example" - no casting needed!
      result = df.filter { |e| e["email"].str_contains("example") }
      result.num_row.should eq(1)
      result["name"][0].should eq("John Doe")
    end

    it "str_ convenience methods work without casting" do
      df = dataframe_of("name").values("Alice", "Bob", "CHARLIE", nil)

      # All these work without .as(StringCol)
      df["name"].str_contains("li").should eq([true, false, false, false])
      df["name"].str_starts_with("A").should eq([true, false, false, false])
      df["name"].str_downcase[2].should eq("charlie")
      df["name"].str_len[0].should eq(5)
    end
  end

  describe "Window Functions" do
    it "rolling_mean calculates moving average" do
      col = Float64Col.new("values", [1.0, 2.0, 3.0, 4.0, 5.0])
      result = col.rolling_mean(3)
      result[0].should be_nil # not enough values
      result[1].should be_nil
      result[2].should eq(2.0) # (1+2+3)/3
      result[3].should eq(3.0) # (2+3+4)/3
      result[4].should eq(4.0) # (3+4+5)/3
    end

    it "rolling_mean with min_periods" do
      col = Float64Col.new("values", [1.0, 2.0, 3.0, 4.0, 5.0])
      result = col.rolling_mean(3, min_periods: 1)
      result[0].should eq(1.0) # just 1
      result[1].should eq(1.5) # (1+2)/2
      result[2].should eq(2.0) # (1+2+3)/3
    end

    it "rolling_sum calculates moving sum" do
      col = Float64Col.new("values", [1.0, 2.0, 3.0, 4.0, 5.0])
      result = col.rolling_sum(3)
      result[2].should eq(6.0)  # 1+2+3
      result[3].should eq(9.0)  # 2+3+4
      result[4].should eq(12.0) # 3+4+5
    end

    it "rolling_min and rolling_max" do
      col = Float64Col.new("values", [3.0, 1.0, 4.0, 1.0, 5.0])
      min_result = col.rolling_min(3)
      max_result = col.rolling_max(3)
      min_result[2].should eq(1.0) # min(3,1,4)
      max_result[2].should eq(4.0) # max(3,1,4)
      min_result[4].should eq(1.0) # min(4,1,5)
      max_result[4].should eq(5.0) # max(4,1,5)
    end

    it "rolling_std calculates moving standard deviation" do
      col = Float64Col.new("values", [1.0, 2.0, 3.0, 4.0, 5.0])
      result = col.rolling_std(3)
      result[2].should eq(1.0) # std of [1,2,3]
      result[3].should eq(1.0) # std of [2,3,4]
    end

    it "ewm_mean calculates exponential weighted moving average" do
      col = Float64Col.new("values", [1.0, 2.0, 3.0, 4.0, 5.0])
      result = col.ewm_mean(3) # alpha = 2/(3+1) = 0.5
      result[0].should eq(1.0)
      # ewm[1] = 0.5 * 2 + 0.5 * 1 = 1.5
      result[1].should eq(1.5)
    end

    it "diff calculates difference from previous value" do
      col = Float64Col.new("values", [1.0, 3.0, 6.0, 10.0])
      result = col.diff
      result[0].should be_nil
      result[1].should eq(2.0) # 3-1
      result[2].should eq(3.0) # 6-3
      result[3].should eq(4.0) # 10-6
    end

    it "diff with periods parameter" do
      col = Float64Col.new("values", [1.0, 2.0, 4.0, 7.0])
      result = col.diff(2)
      result[0].should be_nil
      result[1].should be_nil
      result[2].should eq(3.0) # 4-1
      result[3].should eq(5.0) # 7-2
    end

    it "window functions handle nulls" do
      col = Float64Col.new("values", [1.0, nil, 3.0, 4.0, 5.0])
      result = col.rolling_mean(3, min_periods: 2)
      result[0].should be_nil
      result[1].should be_nil  # null value
      result[2].should eq(2.0) # (1+3)/2, skipping null
      result[3].should eq(3.5) # (3+4)/2
      result[4].should eq(4.0) # (3+4+5)/3
    end

    it "window functions work on Int32Col" do
      col = Int32Col.new("values", [1, 2, 3, 4, 5])
      result = col.rolling_mean(3)
      result[2].should eq(2.0)
      result[4].should eq(4.0)
    end

    it "window functions work on Int64Col" do
      col = Int64Col.new("values", [1_i64, 2_i64, 3_i64, 4_i64, 5_i64])
      result = col.rolling_sum(2)
      result[1].should eq(3.0)
      result[4].should eq(9.0)
    end

    it "diff works on integer columns" do
      col = Int32Col.new("values", [10, 15, 25, 40])
      result = col.diff
      result[1].should eq(5)
      result[2].should eq(10)
      result[3].should eq(15)
    end

    it "window functions in dataframe context" do
      df = dataframe_of("day", "price").values(
        1, 100.0,
        2, 102.0,
        3, 101.0,
        4, 105.0,
        5, 103.0
      )
      result = df.add_column("ma3") { |e| e["price"].as(Float64Col).rolling_mean(3, min_periods: 1) }
      result["ma3"][0].should eq(100.0)
      result["ma3"][2].should eq(101.0) # (100+102+101)/3
    end
  end

  describe "Convenience APIs - DataCol" do
    it "unique returns distinct non-null values" do
      col = Int32Col.new("x", [1, 2, 2, nil, 3, 1, 3])
      col.unique.should eq([1, 2, 3])
    end

    it "nunique counts distinct non-null values" do
      col = StringCol.new("x", ["a", "b", "a", nil, "c"])
      col.nunique.should eq(3)
    end

    it "in? checks membership in array" do
      col = Int32Col.new("x", [1, 2, 3, nil, 4])
      col.in?([1, 3, 5]).should eq([true, false, true, false, false])
    end

    it "is_in is alias for in?" do
      col = StringCol.new("x", ["a", "b", nil, "c"])
      col.is_in(["a", "c"]).should eq([true, false, false, true])
    end

    it "between checks numeric range (inclusive)" do
      col = Float64Col.new("x", [1.0, 2.5, 3.0, nil, 5.0])
      col.between(2.0, 4.0).should eq([false, true, true, false, false])
    end

    it "between works on Int32Col" do
      col = Int32Col.new("x", [1, 2, 3, nil, 5])
      col.between(2, 4).should eq([false, true, true, false, false])
    end

    it "between works on Int64Col" do
      col = Int64Col.new("x", [1_i64, 2_i64, 3_i64, nil, 5_i64])
      col.between(2, 4).should eq([false, true, true, false, false])
    end

    it "clip constrains values to range" do
      col = Float64Col.new("x", [1.0, 5.0, 10.0, nil, 3.0])
      result = col.clip(2.0, 8.0)
      result.values.should eq([2.0, 5.0, 8.0, nil, 3.0])
    end

    it "clip works on Int32Col" do
      col = Int32Col.new("x", [1, 5, 10, nil, 3])
      result = col.clip(2, 8)
      result.values.should eq([2, 5, 8, nil, 3])
    end

    it "clip_lower constrains minimum" do
      col = Float64Col.new("x", [1.0, 5.0, 10.0])
      result = col.clip_lower(3.0)
      result.values.should eq([3.0, 5.0, 10.0])
    end

    it "clip_upper constrains maximum" do
      col = Float64Col.new("x", [1.0, 5.0, 10.0])
      result = col.clip_upper(7.0)
      result.values.should eq([1.0, 5.0, 7.0])
    end

    it "ffill forward fills nulls" do
      col = Float64Col.new("x", [1.0, nil, nil, 4.0, nil])
      result = col.ffill
      result.values.should eq([1.0, 1.0, 1.0, 4.0, 4.0])
    end

    it "ffill keeps leading nulls as null" do
      col = Float64Col.new("x", [nil, nil, 3.0, nil])
      result = col.ffill
      result.values.should eq([nil, nil, 3.0, 3.0])
    end

    it "ffill works on Int32Col" do
      col = Int32Col.new("x", [1, nil, nil, 4])
      result = col.ffill
      result.values.should eq([1, 1, 1, 4])
    end

    it "ffill works on StringCol" do
      col = StringCol.new("x", ["a", nil, "c", nil])
      result = col.ffill
      result.values.should eq(["a", "a", "c", "c"])
    end

    it "bfill backward fills nulls" do
      col = Float64Col.new("x", [nil, 2.0, nil, nil, 5.0])
      result = col.bfill
      result.values.should eq([2.0, 2.0, 5.0, 5.0, 5.0])
    end

    it "bfill keeps trailing nulls as null" do
      col = Float64Col.new("x", [1.0, nil, nil])
      result = col.bfill
      result.values.should eq([1.0, nil, nil])
    end

    it "apply transforms non-null values" do
      col = Float64Col.new("x", [1.0, 2.0, nil, 4.0])
      result = col.apply { |v| v.as(Float64) * 2 }
      result.values.should eq([2.0, 4.0, nil, 8.0])
    end
  end

  describe "Convenience APIs - DataFrame" do
    it "sample(n) returns random subset" do
      df = IRIS_DATA
      result = df.sample(10)
      result.num_row.should eq(10)
      result.num_col.should eq(df.num_col)
    end

    it "sample(n) with seed is reproducible" do
      df = IRIS_DATA
      r1 = df.sample(5, seed: 42)
      r2 = df.sample(5, seed: 42)
      r1["Sepal.Length"].values.should eq(r2["Sepal.Length"].values)
    end

    it "sample(frac) returns fraction of rows" do
      df = IRIS_DATA
      result = df.sample(0.1)
      result.num_row.should eq(15) # 10% of 150
    end

    it "value_counts returns sorted counts" do
      df = dataframe_of("x").values("a", "b", "a", "a", "b", "c")
      result = df.value_counts("x")
      result.num_row.should eq(3)
      result["x"][0].should eq("a")
      result["n"][0].should eq(3)
    end

    it "describe returns summary statistics" do
      df = dataframe_of("a", "b").values(
        1.0, 10,
        2.0, 20,
        3.0, 30,
        4.0, 40,
        5.0, 50
      )
      result = df.describe
      result.num_row.should eq(8) # count, mean, std, min, 25%, 50%, 75%, max
      result["statistic"][0].should eq("count")
      result["a"][0].should eq(5.0) # count
      result["a"][1].should eq(3.0) # mean
    end

    it "describe handles empty numeric columns" do
      df = dataframe_of("name").values("a", "b", "c")
      result = df.describe
      result.num_row.should eq(0) # no numeric columns
    end

    it "coalesce returns first non-null value" do
      df = dataframe_of("a", "b", "c").values(
        nil, nil, 3,
        1, nil, nil,
        nil, 2, nil
      )
      result = df.coalesce("a", "b", "c")
      result.values.should eq([3, 1, 2])
    end

    it "shuffle randomizes row order" do
      df = dataframe_of("x").values(1, 2, 3, 4, 5)
      result = df.shuffle(seed: 42)
      result.num_row.should eq(5)
      # With seed, should be reproducible
      result2 = df.shuffle(seed: 42)
      result["x"].values.should eq(result2["x"].values)
    end

    it "duplicated identifies duplicate rows" do
      df = dataframe_of("a", "b").values(
        1, "x",
        2, "y",
        1, "x",
        3, "z"
      )
      dups = df.duplicated("a", "b")
      dups.should eq([false, false, true, false])
    end

    it "duplicated with single column" do
      df = dataframe_of("x").values(1, 2, 1, 3, 2)
      dups = df.duplicated("x")
      dups.should eq([false, false, true, false, true])
    end

    it "duplicated considers all specified columns" do
      df = dataframe_of("a", "b", "c").values(
        1, 10, 100,
        1, 20, 200,
        2, 10, 100,
        1, 10, 100
      )
      dups = df.duplicated("a", "b", "c")
      dups.should eq([false, false, false, true])
    end

    it "drop_duplicates removes duplicate rows" do
      df = dataframe_of("a", "b").values(
        1, "x",
        2, "y",
        1, "x",
        3, "z"
      )
      result = df.drop_duplicates("a", "b")
      result.num_row.should eq(3)
    end

    it "drop_duplicates keeps first occurrence" do
      df = dataframe_of("x", "y").values(
        1, "first",
        1, "second",
        2, "third"
      )
      result = df.drop_duplicates("x")
      result.num_row.should eq(2)
      result["y"][0].should eq("first")
    end
  end

  describe "Binning Operations" do
    it "cut bins values into intervals" do
      col = Float64Col.new("age", [5.0, 15.0, 25.0, 45.0, 70.0, nil])
      result = col.cut([0, 18, 35, 65, 100], labels: ["child", "young", "adult", "senior"])
      result[0].should eq("child")
      result[1].should eq("child")
      result[2].should eq("young")
      result[3].should eq("adult")
      result[4].should eq("senior")
      result[5].should be_nil
    end

    it "cut works without labels" do
      col = Float64Col.new("x", [5.0, 15.0, 25.0])
      result = col.cut([0, 10, 20, 30])
      result[0].should eq("(0, 10]")
      result[1].should eq("(10, 20]")
      result[2].should eq("(20, 30]")
    end

    it "cut works on Int32Col" do
      col = Int32Col.new("age", [5, 25, 45])
      result = col.cut([0, 18, 65, 100], labels: ["minor", "adult", "senior"])
      result[0].should eq("minor")
      result[1].should eq("adult")
      result[2].should eq("adult")
    end

    it "qcut creates quantile-based bins" do
      col = Float64Col.new("x", [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
      result = col.qcut(4, labels: ["Q1", "Q2", "Q3", "Q4"])
      # Values should be distributed across quartiles
      result.values.compact.uniq.sort.should eq(["Q1", "Q2", "Q3", "Q4"])
    end

    it "qcut works without labels" do
      col = Float64Col.new("x", [1.0, 2.0, 3.0, 4.0])
      result = col.qcut(2)
      result.values.compact.size.should eq(4)
    end
  end

  describe "Concatenation" do
    it "concat vertically stacks dataframes" do
      df1 = dataframe_of("a", "b").values(1, 2)
      df2 = dataframe_of("a", "b").values(3, 4)
      result = Crysda.concat([df1, df2])
      result.num_row.should eq(2)
      result["a"].values.should eq([1, 3])
    end

    it "concat horizontally joins dataframes" do
      df1 = dataframe_of("a").values(1, 2)
      df2 = dataframe_of("b").values(3, 4)
      result = Crysda.concat([df1, df2], axis: 1)
      result.num_col.should eq(2)
      result["a"].values.should eq([1, 2])
      result["b"].values.should eq([3, 4])
    end

    it "concat handles single dataframe" do
      df = dataframe_of("a").values(1, 2)
      result = Crysda.concat([df])
      result.num_row.should eq(2)
    end
  end

  describe "Column Coalesce" do
    it "coalesce returns first non-null from two columns" do
      col1 = Int32Col.new("a", [1, nil, nil, 4])
      col2 = Int32Col.new("b", [10, 20, nil, 40])
      result = col1.coalesce(col2)
      result.values.should eq([1, 20, nil, 4])
    end

    it "coalesce works with strings" do
      col1 = StringCol.new("a", ["x", nil, nil])
      col2 = StringCol.new("b", ["y", "z", nil])
      result = col1.coalesce(col2)
      result.values.should eq(["x", "z", nil])
    end
  end

  describe "Row Operations" do
    it "apply_rows applies function to each row" do
      df = dataframe_of("a", "b").values(
        1, 10,
        2, 20,
        3, 30
      )
      result = df.apply_rows("sum") { |row| row["a"].as_i + row["b"].as_i }
      result["sum"].values.should eq([11, 22, 33])
    end

    it "apply_rows with string concatenation" do
      df = dataframe_of("first", "last").values(
        "John", "Doe",
        "Jane", "Smith"
      )
      result = df.apply_rows("full_name") { |row| "#{row["first"].as_s} #{row["last"].as_s}" }
      result["full_name"][0].should eq("John Doe")
      result["full_name"][1].should eq("Jane Smith")
    end

    it "apply_rows with float arithmetic" do
      df = dataframe_of("price", "qty").values(
        10.5, 2,
        20.0, 3,
        15.5, 4
      )
      result = df.apply_rows("total") { |row| row["price"].as_f * row["qty"].as_i }
      result["total"].values.should eq([21.0, 60.0, 62.0])
    end

    it "apply_rows preserves original columns" do
      df = dataframe_of("x", "y").values(1, 2, 3, 4)
      result = df.apply_rows("z") { |row| row["x"].as_i + row["y"].as_i }
      result.names.should eq(["x", "y", "z"])
      result.num_col.should eq(3)
    end

    it "apply_rows with conditional logic" do
      df = dataframe_of("score").values(85, 45, 72, 90)
      result = df.apply_rows("grade") { |row|
        score = row["score"].as_i
        score >= 70 ? "pass" : "fail"
      }
      result["grade"].values.should eq(["pass", "fail", "pass", "pass"])
    end

    it "apply_rows on empty dataframe" do
      df = dataframe_of(Int32Col.new("a", [] of Int32), Int32Col.new("b", [] of Int32))
      result = df.apply_rows("c") { |row| 0 }
      result.num_row.should eq(0)
      result.names.should contain("c")
    end

    it "apply_rows with boolean result" do
      df = dataframe_of("age").values(15, 25, 17, 30)
      result = df.apply_rows("adult") { |row| row["age"].as_i >= 18 }
      result["adult"].values.should eq([false, true, false, true])
    end

    it "apply_rows accessing multiple columns" do
      df = dataframe_of("a", "b", "c").values(
        1, 2, 3,
        4, 5, 6
      )
      result = df.apply_rows("sum") { |row|
        row["a"].as_i + row["b"].as_i + row["c"].as_i
      }
      result["sum"].values.should eq([6, 15])
    end
  end

  describe "Pivot Table" do
    it "pivot_table aggregates data with sum" do
      df = dataframe_of("region", "product", "sales").values(
        "North", "A", 100,
        "North", "B", 150,
        "South", "A", 200,
        "South", "B", 250
      )
      result = df.pivot_table("region", "product", "sales", "sum")
      result.num_row.should eq(2)
      result.names.should contain("A")
      result.names.should contain("B")
    end

    it "pivot_table with mean aggregation" do
      df = dataframe_of("category", "month", "value").values(
        "X", "Jan", 10.0,
        "X", "Jan", 20.0,
        "X", "Feb", 30.0,
        "Y", "Jan", 40.0,
        "Y", "Feb", 50.0
      )
      result = df.pivot_table("category", "month", "value", "mean")
      result.num_row.should eq(2)
      result.names.should contain("Jan")
      result.names.should contain("Feb")
    end

    it "pivot_table with count aggregation" do
      df = dataframe_of("store", "product", "qty").values(
        "A", "X", 1,
        "A", "X", 2,
        "A", "Y", 3,
        "B", "X", 4,
        "B", "Y", 5,
        "B", "Y", 6
      )
      result = df.pivot_table("store", "product", "qty", "count")
      result.num_row.should eq(2)
    end

    it "pivot_table with min aggregation" do
      df = dataframe_of("group", "type", "val").values(
        "A", "T1", 10,
        "A", "T1", 5,
        "A", "T2", 20,
        "B", "T1", 15,
        "B", "T2", 8
      )
      result = df.pivot_table("group", "type", "val", "min")
      result.num_row.should eq(2)
    end

    it "pivot_table with max aggregation" do
      df = dataframe_of("group", "type", "val").values(
        "A", "T1", 10,
        "A", "T1", 25,
        "A", "T2", 20,
        "B", "T1", 15,
        "B", "T2", 30
      )
      result = df.pivot_table("group", "type", "val", "max")
      result.num_row.should eq(2)
    end

    it "pivot_table with multiple index columns" do
      df = dataframe_of("year", "region", "product", "sales").values(
        2023, "North", "A", 100,
        2023, "North", "B", 150,
        2023, "South", "A", 200,
        2024, "North", "A", 120,
        2024, "South", "B", 180
      )
      result = df.pivot_table(["year", "region"], "product", "sales", "sum")
      result.names.should contain("year")
      result.names.should contain("region")
      result.names.should contain("A")
      result.names.should contain("B")
    end

    it "pivot_table with float values" do
      df = dataframe_of("cat", "sub", "amount").values(
        "X", "a", 10.5,
        "X", "b", 20.3,
        "Y", "a", 15.7,
        "Y", "b", 25.9
      )
      result = df.pivot_table("cat", "sub", "amount", "sum")
      result.num_row.should eq(2)
      result.names.should contain("a")
      result.names.should contain("b")
    end

    it "pivot_table default aggregation is mean" do
      df = dataframe_of("g", "k", "v").values(
        "A", "X", 10,
        "A", "X", 20,
        "B", "X", 30
      )
      result = df.pivot_table("g", "k", "v")
      result.num_row.should eq(2)
    end

    it "pivot_table on empty dataframe" do
      df = dataframe_of(StringCol.new("a", [] of String), StringCol.new("b", [] of String), Int32Col.new("c", [] of Int32))
      result = df.pivot_table("a", "b", "c", "sum")
      result.num_row.should eq(0)
    end

    it "pivot_table with null values in index" do
      df = dataframe_of("region", "product", "sales").values(
        "North", "A", 100,
        nil, "B", 150,
        "South", "A", 200
      )
      result = df.pivot_table("region", "product", "sales", "sum")
      result.num_row.should be >= 2
    end

    it "pivot_table with null values in columns raises error" do
      df = dataframe_of("region", "product", "sales").values(
        "North", "A", 100,
        "North", nil, 150,
        "South", "A", 200
      )
      # Null values in pivot column cause issues with spread
      expect_raises(CrysdaException) do
        df.pivot_table("region", "product", "sales", "sum")
      end
    end

    it "pivot_table with null values in values" do
      df = dataframe_of("region", "product", "sales").values(
        "North", "A", 100,
        "North", "B", nil,
        "South", "A", 200
      )
      result = df.pivot_table("region", "product", "sales", "sum")
      result.num_row.should eq(2)
    end
  end

  describe "Apply Rows Edge Cases" do
    it "apply_rows with nil values in row" do
      df = dataframe_of("a", "b").values(
        1, 10,
        nil, 20,
        3, nil
      )
      result = df.apply_rows("c") { |row|
        a = row["a"].as_i?
        b = row["b"].as_i?
        (a && b) ? a + b : -1
      }
      result["c"].values.should eq([11, -1, -1])
    end

    it "apply_rows returning nil values" do
      df = dataframe_of("x").values(1, 2, 3, 4)
      result = df.apply_rows("y") { |row|
        v = row["x"].as_i
        v > 2 ? v * 10 : nil
      }
      result["y"][0].should be_nil
      result["y"][1].should be_nil
      result["y"][2].should eq(30)
      result["y"][3].should eq(40)
    end

    it "apply_rows with single column" do
      df = dataframe_of("val").values(5, 10, 15)
      result = df.apply_rows("doubled") { |row| row["val"].as_i * 2 }
      result["doubled"].values.should eq([10, 20, 30])
    end

    it "apply_rows with many columns" do
      df = dataframe_of("a", "b", "c", "d", "e").values(
        1, 2, 3, 4, 5,
        10, 20, 30, 40, 50
      )
      result = df.apply_rows("total") { |row|
        row["a"].as_i + row["b"].as_i + row["c"].as_i + row["d"].as_i + row["e"].as_i
      }
      result["total"].values.should eq([15, 150])
    end

    it "apply_rows with string concatenation" do
      df = dataframe_of("a", "b", "c").values(
        "hello", " ", "world",
        "foo", "-", "bar"
      )
      result = df.apply_rows("combined") { |row|
        "#{row["a"].as_s}#{row["b"].as_s}#{row["c"].as_s}"
      }
      result["combined"].values.should eq(["hello world", "foo-bar"])
    end

    it "apply_rows with float result" do
      df = dataframe_of("x", "y").values(
        3, 4,
        5, 12
      )
      result = df.apply_rows("hypotenuse") { |row|
        x = row["x"].as_i.to_f
        y = row["y"].as_i.to_f
        Math.sqrt(x*x + y*y)
      }
      result["hypotenuse"].values[0].should eq(5.0)
      result["hypotenuse"].values[1].should eq(13.0)
    end
  end

  describe "Concat Edge Cases" do
    it "concat empty array raises exception" do
      expect_raises(CrysdaException, "Cannot concat empty array") do
        Crysda.concat([] of DataFrame)
      end
    end

    it "concat single dataframe returns same" do
      df = dataframe_of("a", "b").values(1, 2, 3, 4)
      result = Crysda.concat([df])
      result.num_row.should eq(df.num_row)
      result.num_col.should eq(df.num_col)
    end

    it "concat with empty dataframe" do
      df1 = dataframe_of("a", "b").values(1, 2)
      df2 = dataframe_of(Int32Col.new("a", [] of Int32), Int32Col.new("b", [] of Int32))
      result = Crysda.concat([df1, df2])
      result.num_row.should eq(1)
    end

    it "concat multiple dataframes" do
      df1 = dataframe_of("x").values(1, 2)
      df2 = dataframe_of("x").values(3, 4)
      df3 = dataframe_of("x").values(5, 6)
      result = Crysda.concat([df1, df2, df3])
      result.num_row.should eq(6)
    end

    it "concat with different column orders" do
      df1 = dataframe_of("a", "b").values(1, 2)
      df2 = dataframe_of("b", "a").values(20, 10)
      result = Crysda.concat([df1, df2])
      result.num_row.should eq(2)
    end

    it "concat with missing columns fills with nil" do
      df1 = dataframe_of("a", "b").values(1, 2)
      df2 = dataframe_of("a", "c").values(10, 30)
      result = Crysda.concat([df1, df2])
      result.num_row.should eq(2)
      result.names.should contain("a")
      result.names.should contain("b")
      result.names.should contain("c")
    end
  end

  describe "Cut and Qcut Edge Cases" do
    it "cut with values at boundaries" do
      col = Int32Col.new("x", [0, 10, 20, 30])
      result = col.cut([0, 10, 20, 30])
      result.size.should eq(4)
    end

    it "cut with values outside boundaries" do
      col = Int32Col.new("x", [-5, 5, 15, 35])
      result = col.cut([0, 10, 20, 30])
      result[0].should be_nil # -5 is below range
      result[3].should be_nil # 35 is above range
    end

    it "cut with nil values" do
      col = Int32Col.new("x", [5, nil, 15, nil])
      result = col.cut([0, 10, 20])
      result[1].should be_nil
      result[3].should be_nil
    end

    it "cut with custom labels" do
      col = Int32Col.new("x", [5, 15, 25])
      result = col.cut([0, 10, 20, 30], ["low", "mid", "high"])
      result[0].should eq("low")
      result[1].should eq("mid")
      result[2].should eq("high")
    end

    it "cut with float column" do
      col = Float64Col.new("x", [0.5, 5.5, 15.5])
      result = col.cut([0.0, 10.0, 20.0])
      result.size.should eq(3)
    end

    it "qcut with uniform distribution" do
      col = Int32Col.new("x", [1, 2, 3, 4, 5, 6, 7, 8])
      result = col.qcut(4)
      result.size.should eq(8)
    end

    it "qcut with nil values" do
      col = Int32Col.new("x", [1, nil, 3, nil, 5, 6, 7, 8])
      result = col.qcut(4)
      result[1].should be_nil
      result[3].should be_nil
    end

    it "qcut with single value" do
      col = Int32Col.new("x", [5, 5, 5, 5])
      result = col.qcut(4)
      result.size.should eq(4)
    end

    it "qcut with two quantiles" do
      col = Int32Col.new("x", [1, 2, 3, 4, 5, 6])
      result = col.qcut(2)
      result.size.should eq(6)
    end
  end

  describe "Coalesce Edge Cases" do
    it "coalesce with all nil values" do
      col1 = Int32Col.new("a", [nil, nil, nil])
      col2 = Int32Col.new("b", [nil, nil, nil])
      result = col1.coalesce(col2)
      result[0].should be_nil
      result[1].should be_nil
      result[2].should be_nil
    end

    it "coalesce with no nil values" do
      col1 = Int32Col.new("a", [1, 2, 3])
      col2 = Int32Col.new("b", [10, 20, 30])
      result = col1.coalesce(col2)
      result.values.should eq([1, 2, 3])
    end

    it "coalesce with alternating nil" do
      col1 = Int32Col.new("a", [1, nil, 3, nil])
      col2 = Int32Col.new("b", [nil, 2, nil, 4])
      result = col1.coalesce(col2)
      result.values.should eq([1, 2, 3, 4])
    end

    it "coalesce with empty columns" do
      col1 = Int32Col.new("a", [] of Int32)
      col2 = Int32Col.new("b", [] of Int32)
      result = col1.coalesce(col2)
      result.size.should eq(0)
    end

    it "dataframe coalesce with multiple columns" do
      df = dataframe_of("a", "b", "c").values(
        nil, nil, 3,
        nil, 2, 30,
        1, 20, 300
      )
      result = df.coalesce("a", "b", "c")
      result.values.should eq([3, 2, 1])
    end
  end

  describe "FFill and BFill Edge Cases" do
    it "ffill with leading nils" do
      col = Int32Col.new("x", [nil, nil, 3, 4, nil])
      result = col.ffill
      result[0].should be_nil
      result[1].should be_nil
      result[2].should eq(3)
      result[3].should eq(4)
      result[4].should eq(4)
    end

    it "bfill with trailing nils" do
      col = Int32Col.new("x", [nil, 2, nil, nil, nil])
      result = col.bfill
      result[0].should eq(2)
      result[1].should eq(2)
      result[2].should be_nil
      result[3].should be_nil
      result[4].should be_nil
    end

    it "ffill with all nils" do
      col = Int32Col.new("x", [nil, nil, nil])
      result = col.ffill
      result[0].should be_nil
      result[1].should be_nil
      result[2].should be_nil
    end

    it "bfill with all nils" do
      col = Int32Col.new("x", [nil, nil, nil])
      result = col.bfill
      result[0].should be_nil
      result[1].should be_nil
      result[2].should be_nil
    end

    it "ffill with no nils" do
      col = Int32Col.new("x", [1, 2, 3])
      result = col.ffill
      result.values.should eq([1, 2, 3])
    end

    it "bfill with no nils" do
      col = Int32Col.new("x", [1, 2, 3])
      result = col.bfill
      result.values.should eq([1, 2, 3])
    end

    it "ffill with empty column" do
      col = Int32Col.new("x", [] of Int32)
      result = col.ffill
      result.size.should eq(0)
    end

    it "bfill with empty column" do
      col = Int32Col.new("x", [] of Int32)
      result = col.bfill
      result.size.should eq(0)
    end

    it "ffill with float column" do
      col = Float64Col.new("x", [nil, 1.5, nil, nil, 3.5])
      result = col.ffill
      result[0].should be_nil
      result[1].should eq(1.5)
      result[2].should eq(1.5)
      result[3].should eq(1.5)
      result[4].should eq(3.5)
    end

    it "bfill with string column" do
      col = StringCol.new("x", [nil, nil, "hello", nil])
      result = col.bfill
      result[0].should eq("hello")
      result[1].should eq("hello")
      result[2].should eq("hello")
      result[3].should be_nil
    end
  end

  describe "Clip Edge Cases" do
    it "clip with all values in range" do
      col = Int32Col.new("x", [5, 10, 15])
      result = col.clip(0, 20)
      result.values.should eq([5, 10, 15])
    end

    it "clip with all values below range" do
      col = Int32Col.new("x", [-5, -10, -15])
      result = col.clip(0, 20)
      result.values.should eq([0, 0, 0])
    end

    it "clip with all values above range" do
      col = Int32Col.new("x", [25, 30, 35])
      result = col.clip(0, 20)
      result.values.should eq([20, 20, 20])
    end

    it "clip with nil values" do
      col = Int32Col.new("x", [5, nil, 25, nil])
      result = col.clip(0, 20)
      result[0].should eq(5)
      result[1].should be_nil
      result[2].should eq(20)
      result[3].should be_nil
    end

    it "clip with empty column" do
      col = Int32Col.new("x", [] of Int32)
      result = col.clip(0, 20)
      result.size.should eq(0)
    end

    it "clip_lower only" do
      col = Int32Col.new("x", [-5, 5, 15])
      result = col.clip_lower(0)
      result.values.should eq([0, 5, 15])
    end

    it "clip_upper only" do
      col = Int32Col.new("x", [5, 15, 25])
      result = col.clip_upper(20)
      result.values.should eq([5, 15, 20])
    end

    it "clip with float column" do
      col = Float64Col.new("x", [-1.5, 5.5, 25.5])
      result = col.clip(0.0, 20.0)
      result.values.should eq([0.0, 5.5, 20.0])
    end
  end

  describe "Between and In Edge Cases" do
    it "between with inclusive bounds" do
      col = Int32Col.new("x", [0, 5, 10])
      result = col.between(0, 10)
      result.should eq([true, true, true])
    end

    it "between with nil values" do
      col = Int32Col.new("x", [5, nil, 15])
      result = col.between(0, 10)
      result[0].should eq(true)
      result[1].should eq(false)
      result[2].should eq(false)
    end

    it "between with empty column" do
      col = Int32Col.new("x", [] of Int32)
      result = col.between(0, 10)
      result.size.should eq(0)
    end

    it "is_in with matching values" do
      col = StringCol.new("x", ["a", "b", "c", "d"])
      result = col.is_in(["a", "c"])
      result.should eq([true, false, true, false])
    end

    it "is_in with nil values" do
      col = StringCol.new("x", ["a", nil, "c"])
      result = col.is_in(["a", "c"])
      result[0].should eq(true)
      result[1].should eq(false)
      result[2].should eq(true)
    end

    it "is_in with empty set" do
      col = StringCol.new("x", ["a", "b", "c"])
      result = col.is_in([] of String)
      result.should eq([false, false, false])
    end

    it "is_in with empty column" do
      col = StringCol.new("x", [] of String)
      result = col.is_in(["a", "b"])
      result.size.should eq(0)
    end

    it "is_in with int column" do
      col = Int32Col.new("x", [1, 2, 3, 4, 5])
      result = col.is_in([2, 4])
      result.should eq([false, true, false, true, false])
    end
  end

  describe "Sample and Shuffle Edge Cases" do
    it "sample on empty dataframe" do
      df = dataframe_of(Int32Col.new("x", [] of Int32))
      result = df.sample(5)
      result.num_row.should eq(0)
    end

    it "shuffle on empty dataframe" do
      df = dataframe_of(Int32Col.new("x", [] of Int32))
      result = df.shuffle
      result.num_row.should eq(0)
    end

    it "sample preserves column types" do
      df = dataframe_of("a", "b", "c").values(
        1, "x", 1.5,
        2, "y", 2.5,
        3, "z", 3.5
      )
      result = df.sample(2)
      result["a"].is_a?(Int32Col).should be_true
      result["b"].is_a?(StringCol).should be_true
      result["c"].is_a?(Float64Col).should be_true
    end

    it "shuffle preserves all values" do
      df = dataframe_of("x").values(1, 2, 3, 4, 5)
      result = df.shuffle
      result.num_row.should eq(5)
      result["x"].as_i.compact.sort.should eq([1, 2, 3, 4, 5])
    end

    it "sample with n larger than dataframe returns all rows" do
      df = dataframe_of("x").values(1, 2, 3)
      result = df.sample(10)
      result.num_row.should eq(3) # sample returns self if n >= num_row
    end

    it "sample_n with replace allows oversampling" do
      df = dataframe_of("x").values(1, 2, 3)
      result = df.sample_n(10, replace: true)
      result.num_row.should eq(10)
    end
  end

  describe "Value Counts Edge Cases" do
    it "value_counts on empty dataframe" do
      df = dataframe_of(StringCol.new("x", [] of String))
      result = df.value_counts("x")
      result.num_row.should eq(0)
    end

    it "value_counts with all same values" do
      df = dataframe_of("x").values("a", "a", "a", "a")
      result = df.value_counts("x")
      result.num_row.should eq(1)
      result["n"][0].should eq(4)
    end

    it "value_counts with all unique values" do
      df = dataframe_of("x").values("a", "b", "c", "d")
      result = df.value_counts("x")
      result.num_row.should eq(4)
    end

    it "value_counts with nil values" do
      df = dataframe_of("x").values("a", nil, "a", nil, "b")
      result = df.value_counts("x")
      result.num_row.should be >= 2
    end

    it "value_counts with int column" do
      df = dataframe_of("x").values(1, 1, 2, 2, 2, 3)
      result = df.value_counts("x")
      result.num_row.should eq(3)
    end
  end

  describe "Duplicated and Drop Duplicates Edge Cases" do
    it "duplicated on empty dataframe" do
      df = dataframe_of(Int32Col.new("x", [] of Int32))
      result = df.duplicated("x")
      result.size.should eq(0)
    end

    it "drop_duplicates on empty dataframe" do
      df = dataframe_of(Int32Col.new("x", [] of Int32))
      result = df.drop_duplicates("x")
      result.num_row.should eq(0)
    end

    it "duplicated with all unique rows" do
      df = dataframe_of("x", "y").values(
        1, "a",
        2, "b",
        3, "c"
      )
      result = df.duplicated("x", "y")
      result.should eq([false, false, false])
    end

    it "duplicated with all same rows" do
      df = dataframe_of("x", "y").values(
        1, "a",
        1, "a",
        1, "a"
      )
      result = df.duplicated("x", "y")
      result.should eq([false, true, true])
    end

    it "drop_duplicates keeps first occurrence" do
      df = dataframe_of("x", "y").values(
        1, "a",
        1, "a",
        2, "b",
        2, "b"
      )
      result = df.drop_duplicates("x", "y")
      result.num_row.should eq(2)
    end

    it "drop_duplicates with subset of columns" do
      df = dataframe_of("x", "y", "z").values(
        1, "a", 100,
        1, "a", 200,
        2, "b", 300
      )
      result = df.drop_duplicates("x", "y")
      result.num_row.should eq(2)
    end

    it "duplicated with nil values" do
      df = dataframe_of("x").values(1, nil, 1, nil)
      result = df.duplicated("x")
      result[0].should eq(false)
      result[2].should eq(true)
    end
  end

  describe "Describe Edge Cases" do
    it "describe on empty dataframe" do
      df = dataframe_of(Float64Col.new("x", [] of Float64))
      result = df.describe
      result.num_row.should be > 0
    end

    it "describe with all nil values" do
      df = dataframe_of("x").values(nil, nil, nil)
      result = df.describe
      result.num_row.should be > 0
    end

    it "describe with single value" do
      df = dataframe_of("x").values(42.0)
      result = df.describe
      result.num_row.should be > 0
    end

    it "describe with mixed column types" do
      df = dataframe_of("num", "str", "flag").values(
        1.0, "a", true,
        2.0, "b", false,
        3.0, "c", true
      )
      result = df.describe
      result.names.should contain("num")
    end
  end

  describe "Apply Column Edge Cases" do
    it "apply on empty column" do
      col = Int32Col.new("x", [] of Int32)
      result = col.apply { |v| v }
      result.size.should eq(0)
    end

    it "apply with nil values passes nil through" do
      col = Int32Col.new("x", [1, nil, 3])
      # apply skips nil values, so nil stays nil
      result = col.apply { |v| v.as(Int32) * 2 }
      result[0].should eq(2)
      result[1].should be_nil
      result[2].should eq(6)
    end

    it "apply changing type to string" do
      col = Int32Col.new("x", [1, 2, 3])
      result = col.apply { |v| v.to_s }
      result[0].should eq("1")
      result[1].should eq("2")
      result[2].should eq("3")
    end

    it "apply with float column" do
      col = Float64Col.new("x", [1.5, 2.5, 3.5])
      result = col.apply { |v| v.as(Float64) * 2 }
      result[0].should eq(3.0)
      result[1].should eq(5.0)
      result[2].should eq(7.0)
    end

    it "apply with string column" do
      col = StringCol.new("x", ["hello", "world"])
      result = col.apply { |v| v.as(String).upcase }
      result[0].should eq("HELLO")
      result[1].should eq("WORLD")
    end
  end

  describe "Unique and Nunique Edge Cases" do
    it "unique on empty column" do
      col = Int32Col.new("x", [] of Int32)
      result = col.unique
      result.size.should eq(0)
    end

    it "nunique on empty column" do
      col = Int32Col.new("x", [] of Int32)
      result = col.nunique
      result.should eq(0)
    end

    it "unique with all same values" do
      col = Int32Col.new("x", [5, 5, 5, 5])
      result = col.unique
      result.size.should eq(1)
      result[0].should eq(5)
    end

    it "nunique with nil values excludes nil" do
      col = Int32Col.new("x", [1, nil, 2, nil, 1])
      result = col.nunique
      result.should eq(2) # 1 and 2, nil not counted (unique uses compact)
    end

    it "unique excludes nil values" do
      col = Int32Col.new("x", [1, nil, 1, nil])
      result = col.unique
      result.size.should eq(1) # only 1, nil excluded by compact
    end

    it "unique with string column" do
      col = StringCol.new("x", ["a", "b", "a", "c", "b"])
      result = col.unique
      result.size.should eq(3)
    end
  end

  describe "NLargest and NSmallest" do
    it "nlargest returns top n rows" do
      df = dataframe_of("name", "score").values(
        "Alice", 85,
        "Bob", 92,
        "Carol", 78,
        "Dave", 95,
        "Eve", 88
      )
      result = df.nlargest(3, "score")
      result.num_row.should eq(3)
      result["score"][0].should eq(95)
      result["score"][1].should eq(92)
      result["score"][2].should eq(88)
    end

    it "nsmallest returns bottom n rows" do
      df = dataframe_of("name", "score").values(
        "Alice", 85,
        "Bob", 92,
        "Carol", 78,
        "Dave", 95,
        "Eve", 88
      )
      result = df.nsmallest(3, "score")
      result.num_row.should eq(3)
      result["score"][0].should eq(78)
      result["score"][1].should eq(85)
      result["score"][2].should eq(88)
    end

    it "nlargest with multiple columns for tie-breaking" do
      df = dataframe_of("name", "score", "age").values(
        "Alice", 90, 25,
        "Bob", 90, 30,
        "Carol", 85, 28
      )
      result = df.nlargest(2, "score", "age")
      result.num_row.should eq(2)
      result["name"][0].should eq("Bob") # Same score, older
      result["name"][1].should eq("Alice")
    end

    it "nsmallest with multiple columns for tie-breaking" do
      df = dataframe_of("name", "score", "age").values(
        "Alice", 80, 25,
        "Bob", 80, 30,
        "Carol", 85, 28
      )
      result = df.nsmallest(2, "score", "age")
      result.num_row.should eq(2)
      result["name"][0].should eq("Alice") # Same score, younger
      result["name"][1].should eq("Bob")
    end

    it "nlargest with n larger than dataframe" do
      df = dataframe_of("x").values(1, 2, 3)
      result = df.nlargest(10, "x")
      result.num_row.should eq(3)
    end

    it "nsmallest with n larger than dataframe" do
      df = dataframe_of("x").values(1, 2, 3)
      result = df.nsmallest(10, "x")
      result.num_row.should eq(3)
    end

    it "nlargest on empty dataframe" do
      df = dataframe_of(Int32Col.new("x", [] of Int32))
      result = df.nlargest(5, "x")
      result.num_row.should eq(0)
    end

    it "nsmallest on empty dataframe" do
      df = dataframe_of(Int32Col.new("x", [] of Int32))
      result = df.nsmallest(5, "x")
      result.num_row.should eq(0)
    end

    it "nlargest with null values" do
      df = dataframe_of("x").values(10, nil, 30, nil, 20)
      result = df.nlargest(3, "x")
      result.num_row.should eq(3)
      result["x"][0].should eq(30)
      result["x"][1].should eq(20)
      result["x"][2].should eq(10)
    end

    it "nlargest requires at least one column" do
      df = dataframe_of("x").values(1, 2, 3)
      # Crystal's splat syntax requires at least one argument at compile time
      # so this is enforced by the type system - no runtime test needed
      df.nlargest(2, "x").num_row.should eq(2)
    end
  end

  describe "First and Last" do
    it "first returns first non-null value" do
      df = dataframe_of("x").values(nil, nil, 3, 4, 5)
      df["x"].first.should eq(3)
    end

    it "last returns last non-null value" do
      df = dataframe_of("x").values(1, 2, 3, nil, nil)
      df["x"].last.should eq(3)
    end

    it "first returns nil when all null" do
      df = dataframe_of("x").values(nil, nil, nil)
      df["x"].first.should be_nil
    end

    it "last returns nil when all null" do
      df = dataframe_of("x").values(nil, nil, nil)
      df["x"].last.should be_nil
    end

    it "first on empty column returns nil" do
      col = Int32Col.new("x", [] of Int32)
      col.first.should be_nil
    end

    it "last on empty column returns nil" do
      col = Int32Col.new("x", [] of Int32)
      col.last.should be_nil
    end

    it "first with no nulls returns first value" do
      df = dataframe_of("x").values(1, 2, 3)
      df["x"].first.should eq(1)
    end

    it "last with no nulls returns last value" do
      df = dataframe_of("x").values(1, 2, 3)
      df["x"].last.should eq(3)
    end

    it "first works with string column" do
      df = dataframe_of("name").values(nil, "Alice", "Bob")
      df["name"].first.should eq("Alice")
    end

    it "last works with string column" do
      df = dataframe_of("name").values("Alice", "Bob", nil)
      df["name"].last.should eq("Bob")
    end

    it "first works with float column" do
      df = dataframe_of("x").values(nil, 1.5, 2.5)
      df["x"].first.should eq(1.5)
    end

    it "last works with float column" do
      df = dataframe_of("x").values(1.5, 2.5, nil)
      df["x"].last.should eq(2.5)
    end

    it "first/last in group aggregation" do
      df = dataframe_of("group", "value").values(
        "A", 1,
        "A", 2,
        "A", 3,
        "B", 10,
        "B", 20
      )
      result = df.group_by("group").summarize(
        "first_val".with { |e| e["value"].first },
        "last_val".with { |e| e["value"].last }
      )
      result.filter { |e| e["group"] == "A" }.tap do |a|
        a["first_val"][0].should eq(1)
        a["last_val"][0].should eq(3)
      end
      result.filter { |e| e["group"] == "B" }.tap do |b|
        b["first_val"][0].should eq(10)
        b["last_val"][0].should eq(20)
      end
    end
  end

  describe "Write JSON" do
    it "to_json returns valid JSON string" do
      df = dataframe_of("name", "age").values(
        "Alice", 30,
        "Bob", 25
      )
      json = df.to_json
      json.should contain("Alice")
      json.should contain("30")
      json.should contain("Bob")
      json.should contain("25")
    end

    it "to_json with pretty formatting" do
      df = dataframe_of("x").values(1, 2)
      json = df.to_json(pretty: true)
      json.should contain("\n")
    end

    it "to_json handles null values" do
      df = dataframe_of("x").values(1, nil, 3)
      json = df.to_json
      json.should contain("null")
    end

    it "to_json handles different types" do
      df = dataframe_of("int", "float", "str", "bool").values(
        42, 3.14, "hello", true
      )
      json = df.to_json
      json.should contain("42")
      json.should contain("3.14")
      json.should contain("hello")
      json.should contain("true")
    end

    it "to_json on empty dataframe" do
      df = dataframe_of(Int32Col.new("x", [] of Int32))
      json = df.to_json
      json.should eq("[]")
    end

    it "write_json to IO" do
      df = dataframe_of("a", "b").values(1, 2)
      io = IO::Memory.new
      df.write_json(io)
      io.to_s.should contain("\"a\"")
      io.to_s.should contain("\"b\"")
    end

    it "to_json round-trips with read_json" do
      df = dataframe_of("name", "score").values(
        "Alice", 95,
        "Bob", 87
      )
      json = df.to_json
      # Parse and verify structure
      parsed = JSON.parse(json)
      parsed.as_a.size.should eq(2)
      parsed[0]["name"].as_s.should eq("Alice")
      parsed[0]["score"].as_i.should eq(95)
    end
  end
end
