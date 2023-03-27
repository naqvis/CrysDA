require "./spec_helper"

module Crysda
  class Address < CustomColumnValue
    getter street : String
    getter city : String

    def initialize(@street, @city)
    end

    def to_s
      "#{street}, #{city}"
    end

    def to_s(io : IO) : Nil
      io << to_s
    end

    def hashcode : Int64
      hb = HashBuilder.new
      hb.add(@street).add(@city).hashcode
    end
  end

  describe "Reshape" do
    it "should reshape from int to float" do
      df = dataframe_of("person", "year", "weight", "sex").values(
        "max", 2014, 33.1, "M",
        "max", 2015, 32.3, "M",
        "max", 2016, nil, "M",
        "anna", 2013, 33.5, "F",
        "anna", 2014, 37.3, "F",
        "anna", 2015, 39.2, "F",
        "anna", 2016, 39.9, "F"
      )
      df.schema
      df.spread("year", "weight").tap do |f|
        f.schema
        f.num_row.should eq(2)
        f.num_col.should eq(6) # name, sex, 4 year columns

        # ensure that types were coerced correctly
        f["2013"].is_a?(Float64Col).should be_true
        f["2016"].is_a?(Float64Col).should be_true
      end
    end

    it "should type convert stringified values from string to float" do
      df = dataframe_of("person", "property", "value", "sex").values(
        "max", "salary", "33.1", "M",
        "max", "city", "London", "M",
        "anna", "salary", "33.5", "F",
        "anna", "city", "Berlin", "F"
      )

      df.schema

      df.spread("property", "value", convert: true).tap do |f|
        f.schema
        f.num_row.should eq(2)
        f.num_col.should eq(4) # name, sex, city, salary

        # ensure that types were coerced correctly
        f["city"].is_a?(StringCol).should be_true
        f["salary"].is_a?(Float64Col).should be_true
      end
    end

    it "should gather a numerical matrix into Float format" do
      data = [[1.3, 2.3], [3.9, 7.1]]
      wide_data = data.map_with_index { |d, i| Float64Col.new(i.to_s, d).as(DataCol) }.bind_cols
        .add_row_number("y")

      wide_data.gather("x", "pixel_value", ColumnSelector.new { |x| x.except("y") }).tap do |df|
        df.print
        column_types(df)[2].type.should eq("Float64")
        df.names.should eq(["y", "x", "pixel_value"])
      end
    end

    it "should gather objects as AnyCol" do
      data = dataframe_of("name", "home_address", "work_address").values(
        "John", Address.new("Baker Street", "London"), nil,
        "Anna", Address.new("Mueller Street", "New York"),
        Address.new("Stresemannplatz", "Munich")
      )
      data.gather("type", "address", ColumnSelector.new { |x| x.ends_with?("address") }).tap do |df|
        df.schema
        df.num_col.should eq(3)
        df.names.should eq(["name", "type", "address"])
        column_types(df)[2].type.should eq("Address")
      end
    end

    it "should allow to exclude key columns from gathering" do
      df = dataframe_of("person", "property", "value", "sex").values(
        "max", "salary", "33.1", "M",
        "max", "city", "London", "M",
        "anna", "salary", "33.5", "F",
        "anna", "city", "Berlin", "F"
      )
      wide_df = df.spread("property", "value")

      wide_df.gather("property", "value", ColumnSelector.new { |x| (x.except("person")).and x.starts_with?("person") })

      wide_df.gather("property", "value", ColumnSelector.new { |x| x.except("person") })

      wide_df.gather("property", "value", ColumnSelector.new { |x| x.except("person") })
        .tap do |wf|
          wf.print
          annual_salary = wf.filter { |x| (x["person"] == "anna").and(x["property"] == "salary") }
          annual_salary["value"].values.first.should eq "33.5"
        end
    end

    it "should maintain spread gather equality" do
      df = dataframe_of("person", "property", "value", "sex").values(
        "max", "salary", "33.1", "M",
        "max", "city", "London", "M",
        "anna", "salary", "33.5", "F",
        "anna", "city", "Berlin", "F"
      )
      wide_df = df.spread("property", "value")
      wide_df.gather("property", "value").tap do |wf|
        wf == df
        wf.hash == df.hash
      end
    end

    it "should disallow mixed selections" do
      msg = <<-ERR
Mixing positive and negative selection does not have meaningful semantics and is not supported:
<null>,<null>,<null>,-order,<null>,+sleep_total,+sleep_rem,+sleep_cycle,<null>,<null>,<null>
ERR
      expect_raises(InvalidColumnSelectException, msg) do
        SLEEP_DATA.gather("foo", "bar", ColumnSelector.new { |x| (x.except("order")).and x.starts_with?("sleep") })
      end
    end
  end

  it "should spread and unite columns" do
    SLEEP_DATA.unite("test", ["name", "order"], remove: false).tap do |df|
      df.take.print
      df.names.includes?("name").should be_true
      df["test"].size.should eq(df.num_row)
    end

    SLEEP_DATA.unite("test", ["name", "order"]).tap do |df|
      df.take.print
      df.names.includes?("name").should be_false
      df.names.includes?("order").should be_false
      df["test"].size.should eq(df.num_row)
    end

    united = SLEEP_DATA.unite("test", ColumnSelector.new { |c| c.list_of(["name", "sleep_rem"]) }, sep: ",")

    united.separate("test", ["new_name", "new_sleep_rem"], convert: true, sep: ",").tap do |df|
      df.take.print
      df.schema

      df["new_name"] == SLEEP_DATA["name"]
      df["new_sleep_rem"] == SLEEP_DATA["sleep_rem"]
    end
  end

  it "nest grouped data" do
    schema = <<-STR
    DataFrame with 3 observations
    Species [String]    setosa, versicolor, virginica
    data    [DataFrame] <DataFrame [50 x 4]>, <DataFrame [50 x 4]>, <DataFrame [50 x 4]>
    STR
    IRIS_DATA.group_by("Species")
      .nest.tap do |df|
      df.print
      df.num_row.should eq(3)
      df.num_col.should eq(2)
      df.names.should eq(["Species", "data"])
      io = IO::Memory.new
      df.schema(output: io)
      io.to_s.rchop.should eq(schema)
    end
  end

  it "nest selected columns only" do
    IRIS_DATA.nest(ColumnSelector.new { |c| c.except("Species") }).tap do |df|
      df.schema
      df.num_row.should eq 3
      df.num_col.should eq 2
      df.names.should eq(["Species", "data"])
    end
  end

  it "should unnest data" do
    # use other small but NA-heavy data set here
    restored = SLEEP_DATA
      .nest(ColumnSelector.new { |c| c.except("order") })
      .unnest(DataFrame::DEF_NEST_COLUMN_NAME)
      .sort_by("order")
      .move_left("name", "genus", "vore")

    restored.tap do |df|
      df.print

      df.num_row.should eq(SLEEP_DATA.num_row)
      df.num_col.should eq(SLEEP_DATA.num_col)
      df.names.should eq(SLEEP_DATA.names)
    end

    restored == SLEEP_DATA.sort_by("order")
  end

  it "should unnest list columns" do
    df = dataframe_of("id", "tags").values(
      "foo", List(String).of("some", "tags"),
      "bar", List(String).of("some", "other", "tags")
    )
    df.unnest("tags").tap do |d|
      d.print
      d.num_row.should eq(5)
    end
  end

  it "should expand variable tuples like tidyr-expand" do
    df = dataframe_of("person", "year", "weight", "sex").values(
      "max", 2014, 33.1, "M",
      "max", 2016, nil, "M",
      "anna", 2015, 39.2, "F",
      "anna", 2016, 39.9, "F")
    df.print
    df.expand("year", "sex").tap do |d|
      d.print
      d.num_row.should eq(6)
      d.names.should eq(["year", "sex"])
    end

    df.complete("year", "person").tap do |d|
      d.print
      d.num_row.should eq(6)
      d.num_col.should eq(4)
      d.filter { |f| f["weight"].is_na }.num_row.should eq(3)
    end

    # next steps in here: implement test nesting support ...
  end

  it "should add a new row" do
    df = dataframe_of("Col1", "Col2").values(
      "Row1", 1,
      "Row2", 2,
      "Row3", 3
    )
    df.print
    df = df.add_row("Row4", 4)

    # No columns should added/removed
    df.num_col.should eq(2)

    # Number of rows should be one more than the initial number
    df.num_row.should eq(4)

    # Col1 of dataframe should have the previous values plus the new one
    df["Col1"].values.should eq(["Row1", "Row2", "Row3", "Row4"])

    # Col2 of dataframe should have the previous values plus the new one
    df["Col2"].values.should eq([1, 2, 3, 4])

    # Col2 of the ne row should contain the new value
    df.filter { |f| f["Col1"] == "Row4" }["Col2"].values.should eq([4])
    df.print
  end
end
