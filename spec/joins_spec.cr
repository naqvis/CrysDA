require "./spec_helper"

module Crysda
  persons = dataframe_of(
    "first_name", "last_name", "age").values(
    "max", "smith", 53,
    "tom", "doe", 30,
    "eva", "miller", 23
  )

  weights = dataframe_of(
    "first", "last", "weight").values(
    "max", "smith", 56.3,
    "tom", "doe", nil,
    "eva", "meyer", 23.3
  )

  describe "Joins" do
    it "test constrained cartesian products" do
      a = dataframe_of("name", "project_id").values(
        "Max", "P1",
        "Max", "P2",
        "Tom", "P3"
      )

      b = dataframe_of("title", "project_id").values(
        "foo", "P1",
        "some_title", "P2",
        "alt_title", "P2"
      )

      cp = DataFrame.cartesian_product(a.reject("project_id"), b)
      cp.print
      cp.num_row.should eq(9)
      cp["name"][0].should eq("Max")
      cp["name"][1].should eq("Max")
      cp["name"][2].should eq("Tom")

      cp["title"][0].should eq("foo")
      cp["title"][1].should eq("foo")
      cp["title"][2].should eq("foo")
    end

    it "should perform an inner join" do
      vore_info = SLEEP_DATA.group_by("vore").summarize("vore_mod".with { |v| v["vore"].as_s.first?.try &.+ "__2" })
      vore_info.print

      # auto detect 'by' here
      sleep_with_info = SLEEP_DATA.left_join(vore_info)
      sleep_with_info.schema

      sleep_with_info.num_row.should eq(SLEEP_DATA.num_row)
      # make sure that by columns don't show up twice
      sleep_with_info.num_col.should eq(SLEEP_DATA.num_col + 1)

      sleep_with_info.take.print
    end

    it "should allow to join by all columns" do
      SLEEP_DATA.inner_join(SLEEP_DATA).names.should eq(SLEEP_DATA.names)
    end

    it "should allow with actually equal bys in unequal mode" do
      SLEEP_DATA.inner_join(
        SLEEP_DATA.rename({name: "order", with: "new_order"}),
        by: [{"vore", "vore"}, {"order", "new_order"}]).num_row.should eq(597)
    end

    it "should no-overlap data should still return correct column model" do
      SLEEP_DATA.inner_join(
        IRIS_DATA.add_column("vore".with { |_| "foobar" }),
        by: "vore")
        .tap do |df|
          df.names.size.should be > 15
          df.num_row.should eq(0)
        end
    end

    it "it should add suffices if join column names have duplicates" do
      df = dataframe_of("foo", "bar").values(
        "a", 2,
        "b", 3,
        "c", 4
      )

      # join on foo
      df.inner_join(df, by: "foo", suffices: {"_1", "_2"}).tap do |d|
        d.print
        d.names.should eq(["foo", "bar_1", "bar_2"])
      end

      # again but now join on bar. Join columns are expected to come first
      df.inner_join(df, "bar", {"_1", "_2"}).tap do |d|
        d.names.should eq(["bar", "foo_1", "foo_2"])
      end

      # again but now join on nothing
      df.inner_join(df, [] of String, {"_1", "_2"}).tap do |d|
        d.num_row.should eq(0)
        d.names.should eq(["foo_1", "bar_1", "foo_2", "bar_2"])
      end
    end

    it "it should allow to use different and multiple by columns" do
      persons.rename({name: "last_name", with: "name"})
        .inner_join(weights, by: [{"name", "last"}])
        .tap do |df|
          df.print
          df.num_row.should eq(2)
        end
    end

    it "outer join - it should join calculate cross-product when joining on empty by list" do
      df = dataframe_of("foo", "bar").values(
        "a", 2,
        "b", 3,
        "c", 4
      )
      df.outer_join(df, by: [] of String).tap do |d|
        d.print
        d.num_row.should eq 6
        d.num_col.should eq 4
        d.names.should eq(["foo.x", "bar.x", "foo.y", "bar.y"])
      end
    end

    it "should retain join keys of non-matching LHS records in outer join" do
      user = dataframe_of("first_name", "last_name", "age", "weight").values(
        "Max", "Doe", 23, 55,
        "Franz", "Smith", 23, 88,
        "Horst", "Keanes", 12, 82
      )

      pets = dataframe_of("first_name", "pet").values(
        "Max", "Cat",
        "Franz", "Dog",
        # no pet for Horst
        "Uwe", "Elephant" # Uwe is not in user dataframe
      )

      user.outer_join(pets).tap do |df|
        df.print
        # df.filter { |f| f["pet"] == "Elephant" }["first_name"][0].should eq("Uwe")
      end
    end

    it "should join empty dataframe" do
      user = dataframe_of("first_name", "last_name", "age", "weight").values(
        "Max", "Doe", 23, 55
      )

      pets = dataframe_of("first_name", "pet").values(
        "Max", "Cat",
        "Franz", "Dog",
      )
      join_by = ["first_name"]

      # Hans does not exists
      user.filter { |f| f["first_name"] == "Hans" }.print
      user.filter { |f| f["first_name"] == "Hans" }.left_join(pets, join_by).tap do |df|
        df.print("left join result")
        df.num_row.should eq(0)
        df.names.should eq(["first_name", "last_name", "age", "weight", "pet"])
      end

      user.filter { |f| f["first_name"] == "Hans" }.outer_join(pets, join_by).tap do |df|
        df.print("right join result")
        df.names.should eq(["first_name", "last_name", "age", "weight", "pet"])
        df.num_row.should eq(2)
        df["first_name"].values.should eq(["Max", "Franz"])
      end
    end

    it "semi join - should join calculate cross-product when joining on empty by list" do
      df = dataframe_of("foo", "bar").values(
        "a", 2,
        "b", 3,
        "c", 4
      )

      filter = dataframe_of("foo", "bar").values(
        "a", 3.2,
        "a", 1.1,
        "b", 3.0,
        "d", 3.2
      )

      df.semi_join(filter, by: "foo").tap do |d|
        d.num_row.should eq(0)
        d.num_col.should eq(2)

        # make sure that renaming does not kick in
        d.names.should eq(["foo", "bar"])
      end
    end
  end
end
