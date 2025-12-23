require "./spec_helper"

module Crysda
  describe "NullBitmap" do
    it "should track nulls correctly" do
      bm = NullBitmap.new(100)
      bm.any?.should eq(false)
      bm.none?.should eq(true)
      bm.count.should eq(0)

      bm.set(5)
      bm.set(50)
      bm.set(99)

      bm.any?.should eq(true)
      bm.none?.should eq(false)
      bm.count.should eq(3)

      bm[5].should eq(true)
      bm[50].should eq(true)
      bm[99].should eq(true)
      bm[0].should eq(false)
      bm[6].should eq(false)
    end

    it "should combine bitmaps with OR" do
      bm1 = NullBitmap.new(10)
      bm1.set(1)
      bm1.set(3)

      bm2 = NullBitmap.new(10)
      bm2.set(2)
      bm2.set(3)

      combined = bm1 | bm2
      combined[0].should eq(false)
      combined[1].should eq(true)
      combined[2].should eq(true)
      combined[3].should eq(true)
      combined[4].should eq(false)
      combined.count.should eq(3)
    end

    it "should create from nullable array" do
      arr = [1.0, nil, 3.0, nil, 5.0] of Float64?
      bm = NullBitmap.from_nullable(arr)

      bm.count.should eq(2)
      bm[0].should eq(false)
      bm[1].should eq(true)
      bm[2].should eq(false)
      bm[3].should eq(true)
      bm[4].should eq(false)
    end

    it "should handle large bitmaps spanning multiple words" do
      bm = NullBitmap.new(200)
      bm.set(0)
      bm.set(63)  # last bit of first word
      bm.set(64)  # first bit of second word
      bm.set(127) # last bit of second word
      bm.set(199) # near end

      bm.count.should eq(5)
      bm[0].should eq(true)
      bm[63].should eq(true)
      bm[64].should eq(true)
      bm[127].should eq(true)
      bm[199].should eq(true)
      bm[1].should eq(false)
      bm[65].should eq(false)
    end
  end

  describe "Optimized Float64Col" do
    it "should store data efficiently with no nulls" do
      col = Float64Col.new("test", [1.0, 2.0, 3.0, 4.0, 5.0] of Float64?)
      col.has_nulls?.should eq(false)
      col.values.should eq([1.0, 2.0, 3.0, 4.0, 5.0])
      col.size.should eq(5)
    end

    it "should handle nulls correctly" do
      col = Float64Col.new("test", [1.0, nil, 3.0, nil, 5.0] of Float64?)
      col.has_nulls?.should eq(true)
      col.values.should eq([1.0, nil, 3.0, nil, 5.0])
      col[0].should eq(1.0)
      col[1].should eq(nil)
      col[2].should eq(3.0)
    end

    it "should create dense column without nulls" do
      col = Float64Col.dense("test", [1.0, 2.0, 3.0])
      col.has_nulls?.should eq(false)
      col.values.should eq([1.0, 2.0, 3.0])
    end

    describe "aggregations without nulls (fast path)" do
      col = Float64Col.new("test", [1.0, 2.0, 3.0, 4.0, 5.0] of Float64?)

      it "should compute sum" do
        col.opt_sum.should eq(15.0)
      end

      it "should compute mean" do
        col.opt_mean.should eq(3.0)
      end

      it "should compute min" do
        col.opt_min.should eq(1.0)
      end

      it "should compute max" do
        col.opt_max.should eq(5.0)
      end

      it "should compute median (odd count)" do
        col.opt_median.should eq(3.0)
      end

      it "should compute median (even count)" do
        col2 = Float64Col.new("test", [1.0, 2.0, 3.0, 4.0] of Float64?)
        col2.opt_median.should eq(2.5)
      end

      it "should compute standard deviation" do
        col.opt_sd.should be_close(1.5811, 0.001)
      end
    end

    describe "aggregations with nulls (slow path)" do
      col = Float64Col.new("test", [1.0, nil, 3.0, nil, 5.0] of Float64?)

      it "should raise without remove_na" do
        expect_raises(MissingValueException) { col.opt_sum }
        expect_raises(MissingValueException) { col.opt_mean }
        expect_raises(MissingValueException) { col.opt_min }
        expect_raises(MissingValueException) { col.opt_max }
        expect_raises(MissingValueException) { col.opt_median }
        expect_raises(MissingValueException) { col.opt_sd }
      end

      it "should compute sum with remove_na" do
        col.opt_sum(remove_na: true).should eq(9.0)
      end

      it "should compute mean with remove_na" do
        col.opt_mean(remove_na: true).should eq(3.0)
      end

      it "should compute min with remove_na" do
        col.opt_min(remove_na: true).should eq(1.0)
      end

      it "should compute max with remove_na" do
        col.opt_max(remove_na: true).should eq(5.0)
      end

      it "should compute median with remove_na" do
        col.opt_median(remove_na: true).should eq(3.0)
      end

      it "should compute sd with remove_na" do
        col.opt_sd(remove_na: true).should eq(2.0)
      end
    end

    describe "arithmetic operations" do
      it "should add columns (fast path - no nulls)" do
        a = Float64Col.new("a", [1.0, 2.0, 3.0] of Float64?)
        b = Float64Col.new("b", [4.0, 5.0, 6.0] of Float64?)
        result = a + b
        result.values.should eq([5.0, 7.0, 9.0])
        result.has_nulls?.should eq(false)
      end

      it "should add columns (slow path - with nulls)" do
        a = Float64Col.new("a", [1.0, nil, 3.0] of Float64?)
        b = Float64Col.new("b", [4.0, 5.0, nil] of Float64?)
        result = a + b
        result.values.should eq([5.0, nil, nil])
        result.has_nulls?.should eq(true)
      end

      it "should subtract columns" do
        a = Float64Col.new("a", [5.0, 6.0, 7.0] of Float64?)
        b = Float64Col.new("b", [1.0, 2.0, 3.0] of Float64?)
        result = a - b
        result.values.should eq([4.0, 4.0, 4.0])
      end

      it "should multiply columns" do
        a = Float64Col.new("a", [2.0, 3.0, 4.0] of Float64?)
        b = Float64Col.new("b", [3.0, 4.0, 5.0] of Float64?)
        result = a * b
        result.values.should eq([6.0, 12.0, 20.0])
      end

      it "should divide columns" do
        a = Float64Col.new("a", [10.0, 20.0, 30.0] of Float64?)
        b = Float64Col.new("b", [2.0, 4.0, 5.0] of Float64?)
        result = a / b
        result.values.should eq([5.0, 5.0, 6.0])
      end

      it "should add scalar" do
        col = Float64Col.new("a", [1.0, 2.0, 3.0] of Float64?)
        result = col + 10.0
        result.values.should eq([11.0, 12.0, 13.0])
      end

      it "should preserve nulls with scalar operations" do
        col = Float64Col.new("a", [1.0, nil, 3.0] of Float64?)
        result = col + 10.0
        result.values.should eq([11.0, nil, 13.0])
      end
    end

    describe "comparison operations" do
      it "should compare with scalar (fast path)" do
        col = Float64Col.new("a", [1.0, 2.0, 3.0, 4.0, 5.0] of Float64?)
        (col > 3.0).should eq([false, false, false, true, true])
        (col >= 3.0).should eq([false, false, true, true, true])
        (col < 3.0).should eq([true, true, false, false, false])
        (col <= 3.0).should eq([true, true, true, false, false])
        (col == 3.0).should eq([false, false, true, false, false])
      end

      it "should compare with scalar (slow path - with nulls)" do
        col = Float64Col.new("a", [1.0, nil, 3.0, nil, 5.0] of Float64?)
        (col > 2.0).should eq([false, false, true, false, true])
        (col < 4.0).should eq([true, false, true, false, false])
      end
    end
  end

  describe "Optimized Int32Col" do
    it "should store data efficiently with no nulls" do
      col = Int32Col.new("test", [1, 2, 3, 4, 5] of Int32?)
      col.has_nulls?.should eq(false)
      col.values.should eq([1, 2, 3, 4, 5])
    end

    it "should handle nulls correctly" do
      col = Int32Col.new("test", [1, nil, 3, nil, 5] of Int32?)
      col.has_nulls?.should eq(true)
      col.values.should eq([1, nil, 3, nil, 5])
    end

    describe "aggregations without nulls" do
      col = Int32Col.new("test", [1, 2, 3, 4, 5] of Int32?)

      it "should compute sum" do
        col.opt_sum.should eq(15_i64)
      end

      it "should compute mean" do
        col.opt_mean.should eq(3.0)
      end

      it "should compute min" do
        col.opt_min.should eq(1)
      end

      it "should compute max" do
        col.opt_max.should eq(5)
      end

      it "should compute median" do
        col.opt_median.should eq(3.0)
      end
    end

    describe "aggregations with nulls" do
      col = Int32Col.new("test", [1, nil, 3, nil, 5] of Int32?)

      it "should compute sum with remove_na" do
        col.opt_sum(remove_na: true).should eq(9_i64)
      end

      it "should compute mean with remove_na" do
        col.opt_mean(remove_na: true).should eq(3.0)
      end
    end

    describe "arithmetic operations" do
      it "should add Int32 columns" do
        a = Int32Col.new("a", [1, 2, 3] of Int32?)
        b = Int32Col.new("b", [4, 5, 6] of Int32?)
        result = a + b
        result.values.should eq([5, 7, 9])
      end

      it "should handle null propagation" do
        a = Int32Col.new("a", [1, nil, 3] of Int32?)
        b = Int32Col.new("b", [4, 5, nil] of Int32?)
        result = a + b
        result.values.should eq([5, nil, nil])
      end

      it "should divide and return Float64Col" do
        a = Int32Col.new("a", [10, 20, 30] of Int32?)
        b = Int32Col.new("b", [2, 4, 5] of Int32?)
        result = a / b
        result.should be_a(Float64Col)
        result.values.should eq([5.0, 5.0, 6.0])
      end
    end
  end

  describe "Optimized Int64Col" do
    it "should store data efficiently" do
      col = Int64Col.new("test", [1_i64, 2_i64, 3_i64] of Int64?)
      col.has_nulls?.should eq(false)
      col.values.should eq([1_i64, 2_i64, 3_i64])
    end

    it "should handle nulls" do
      col = Int64Col.new("test", [1_i64, nil, 3_i64] of Int64?)
      col.has_nulls?.should eq(true)
      col.values.should eq([1_i64, nil, 3_i64])
    end

    describe "aggregations" do
      col = Int64Col.new("test", [1_i64, 2_i64, 3_i64, 4_i64, 5_i64] of Int64?)

      it "should compute sum" do
        col.opt_sum.should eq(15_i64)
      end

      it "should compute mean" do
        col.opt_mean.should eq(3.0)
      end

      it "should compute min" do
        col.opt_min.should eq(1_i64)
      end

      it "should compute max" do
        col.opt_max.should eq(5_i64)
      end
    end
  end

  describe "Optimized StringCol" do
    it "should store data efficiently with no nulls" do
      col = StringCol.new("test", ["a", "b", "c"] of String?)
      col.has_nulls?.should eq(false)
      col.values.should eq(["a", "b", "c"])
    end

    it "should handle nulls correctly" do
      col = StringCol.new("test", ["a", nil, "c"] of String?)
      col.has_nulls?.should eq(true)
      col.values.should eq(["a", nil, "c"])
      col[0].should eq("a")
      col[1].should eq(nil)
      col[2].should eq("c")
    end

    describe "comparison operations" do
      it "should compare with scalar (fast path)" do
        col = StringCol.new("test", ["a", "b", "c", "d"] of String?)
        (col > "b").should eq([false, false, true, true])
        (col < "c").should eq([true, true, false, false])
      end

      it "should compare with scalar (slow path - with nulls)" do
        col = StringCol.new("test", ["a", nil, "c", nil] of String?)
        (col > "a").should eq([false, false, true, false])
      end
    end
  end

  describe "DataFrame operations with optimized columns" do
    it "should filter preserving null handling" do
      df = dataframe_of("x", "y").values(
        1.0, "a",
        nil, "b",
        3.0, "c",
        nil, "d",
        5.0, "e"
      )

      filtered = df.filter { |e| e["x"] > 2.0 }
      filtered.num_row.should eq(2)
      filtered["x"].values.should eq([3.0, 5.0])
      filtered["y"].values.should eq(["c", "e"])
    end

    it "should sort preserving null handling" do
      df = dataframe_of("x").values(3.0, nil, 1.0, nil, 2.0)
      sorted = df.sort_by("x")
      sorted["x"].values.should eq([1.0, 2.0, 3.0, nil, nil])
    end

    it "should group_by and summarize with optimized aggregations" do
      df = dataframe_of("group", "value").values(
        "a", 1.0,
        "a", 2.0,
        "a", 3.0,
        "b", 10.0,
        "b", 20.0
      )

      result = df.group_by("group").summarize(
        "sum".with { |e| e["value"].sum },
        "mean".with { |e| e["value"].mean }
      )

      result.num_row.should eq(2)
      a_row = result.filter { |e| e["group"] == "a" }
      a_row["sum"][0].should eq(6.0)
      a_row["mean"][0].should eq(2.0)

      b_row = result.filter { |e| e["group"] == "b" }
      b_row["sum"][0].should eq(30.0)
      b_row["mean"][0].should eq(15.0)
    end

    it "should handle column arithmetic in add_column" do
      df = dataframe_of("a", "b").values(
        1.0, 2.0,
        3.0, 4.0,
        5.0, 6.0
      )

      result = df.add_column("c") { |e| e["a"] + e["b"] }
      result["c"].values.should eq([3.0, 7.0, 11.0])
    end

    it "should handle mixed null arithmetic in add_column" do
      df = dataframe_of("a", "b").values(
        1.0, 2.0,
        nil, 4.0,
        5.0, nil
      )

      result = df.add_column("c") { |e| e["a"] + e["b"] }
      result["c"].values.should eq([3.0, nil, nil])
    end
  end

  describe "DataCol base class dispatch" do
    it "should dispatch sum to optimized method" do
      col : DataCol = Float64Col.new("test", [1.0, 2.0, 3.0] of Float64?)
      col.sum.should eq(6.0)
    end

    it "should dispatch mean to optimized method" do
      col : DataCol = Int32Col.new("test", [2, 4, 6] of Int32?)
      col.mean.should eq(4.0)
    end

    it "should dispatch min/max to optimized methods" do
      col : DataCol = Int64Col.new("test", [5_i64, 1_i64, 9_i64] of Int64?)
      col.min.should eq(1_i64)
      col.max.should eq(9_i64)
    end
  end

  describe "StringPool (String Interning)" do
    it "should intern strings and return same instance" do
      pool = StringPool.new
      s1 = pool.intern("hello")
      s2 = pool.intern("hello")
      s3 = pool.intern("world")

      # Same string should return same object
      s1.object_id.should eq(s2.object_id)
      s1.object_id.should_not eq(s3.object_id)
    end

    it "should track pool size" do
      pool = StringPool.new
      pool.intern("a")
      pool.intern("b")
      pool.intern("a") # duplicate
      pool.intern("c")

      pool.size.should eq(3)
    end

    it "should handle nil strings" do
      pool = StringPool.new
      result = pool.intern(nil)
      result.should eq(nil)
    end
  end

  describe "StringCol interning" do
    it "should create interned column" do
      col = StringCol.interned("test", ["a", "b", "a", "c", "a", "b"] of String?)
      col.values.should eq(["a", "b", "a", "c", "a", "b"])
      col.unique_count.should eq(3)
    end

    it "should detect categorical columns" do
      # Low cardinality - categorical
      categorical = StringCol.new("cat", ["a", "a", "a", "b", "b", "a"] of String?)
      categorical.categorical?.should eq(true)

      # High cardinality - not categorical
      unique = StringCol.new("unique", ["a", "b", "c", "d", "e", "f"] of String?)
      unique.categorical?.should eq(false)
    end
  end

  describe "Optimized group_by hashing" do
    it "should group by numeric columns efficiently" do
      df = dataframe_of("group", "value").values(
        1, 10.0,
        1, 20.0,
        2, 30.0,
        2, 40.0,
        1, 50.0
      )

      grouped = df.group_by("group")
      grouped.groups.size.should eq(2)

      # Check group contents
      g1 = grouped.groups.find { |g| g["group"][0] == 1 }
      g1.not_nil!.num_row.should eq(3)

      g2 = grouped.groups.find { |g| g["group"][0] == 2 }
      g2.not_nil!.num_row.should eq(2)
    end

    it "should handle null values in grouping" do
      df = dataframe_of("group", "value").values(
        "a", 1,
        nil, 2,
        "a", 3,
        nil, 4,
        "b", 5
      )

      grouped = df.group_by("group")
      grouped.groups.size.should eq(3) # "a", "b", and nil
    end

    it "should handle multi-column grouping" do
      df = dataframe_of("g1", "g2", "value").values(
        "a", 1, 10,
        "a", 1, 20,
        "a", 2, 30,
        "b", 1, 40
      )

      grouped = df.group_by("g1", "g2")
      grouped.groups.size.should eq(3)
    end
  end

  describe "NullBitmap advanced operations" do
    it "should support AND operation" do
      bm1 = NullBitmap.new(10)
      bm1.set(1)
      bm1.set(2)
      bm1.set(3)

      bm2 = NullBitmap.new(10)
      bm2.set(2)
      bm2.set(3)
      bm2.set(4)

      combined = bm1 & bm2
      combined[1].should eq(false)
      combined[2].should eq(true)
      combined[3].should eq(true)
      combined[4].should eq(false)
      combined.count.should eq(2)
    end

    it "should support clear operation" do
      bm = NullBitmap.new(10)
      bm.set(5)
      bm[5].should eq(true)

      bm.clear(5)
      bm[5].should eq(false)
    end

    it "should build from block" do
      bm = NullBitmap.build(10) { |i| i % 2 == 0 }
      bm[0].should eq(true)
      bm[1].should eq(false)
      bm[2].should eq(true)
      bm[3].should eq(false)
      bm.count.should eq(5)
    end

    it "should handle SIMD-sized bitmaps (>= 256 bits)" do
      # Create bitmap large enough to trigger SIMD path (>= 4 words = 256 bits)
      bm = NullBitmap.new(500)

      # Set bits across multiple words
      bm.set(0)
      bm.set(63)  # end of word 0
      bm.set(64)  # start of word 1
      bm.set(127) # end of word 1
      bm.set(200) # word 3
      bm.set(300) # word 4
      bm.set(499) # near end

      bm.count.should eq(7)
      bm[0].should eq(true)
      bm[63].should eq(true)
      bm[64].should eq(true)
      bm[127].should eq(true)
      bm[200].should eq(true)
      bm[300].should eq(true)
      bm[499].should eq(true)
      bm[1].should eq(false)
      bm[100].should eq(false)
    end

    it "should OR large bitmaps correctly (SIMD path)" do
      bm1 = NullBitmap.new(500)
      bm1.set(10)
      bm1.set(100)
      bm1.set(200)

      bm2 = NullBitmap.new(500)
      bm2.set(50)
      bm2.set(100)
      bm2.set(300)

      combined = bm1 | bm2
      combined[10].should eq(true)
      combined[50].should eq(true)
      combined[100].should eq(true)
      combined[200].should eq(true)
      combined[300].should eq(true)
      combined[0].should eq(false)
      combined.count.should eq(5)
    end

    it "should AND large bitmaps correctly (SIMD path)" do
      bm1 = NullBitmap.new(500)
      bm1.set(100)
      bm1.set(200)
      bm1.set(300)

      bm2 = NullBitmap.new(500)
      bm2.set(100)
      bm2.set(250)
      bm2.set(300)

      combined = bm1 & bm2
      combined[100].should eq(true)
      combined[200].should eq(false)
      combined[250].should eq(false)
      combined[300].should eq(true)
      combined.count.should eq(2)
    end
  end

  describe "Lazy iteration (Float64Col)" do
    it "should iterate lazily without materializing array" do
      col = Float64Col.new("test", [1.0, 2.0, 3.0, 4.0, 5.0] of Float64?)

      sum = 0.0
      col.each { |v| sum += v.not_nil! }
      sum.should eq(15.0)
    end

    it "should iterate with index" do
      col = Float64Col.new("test", [10.0, 20.0, 30.0] of Float64?)

      pairs = [] of {Float64?, Int32}
      col.each_with_index { |v, i| pairs << {v, i} }

      pairs.should eq([{10.0, 0}, {20.0, 1}, {30.0, 2}])
    end

    it "should iterate over non-null values only (fast path)" do
      col = Float64Col.new("test", [1.0, 2.0, 3.0] of Float64?)

      values = [] of Float64
      col.each_non_null { |v| values << v }
      values.should eq([1.0, 2.0, 3.0])
    end

    it "should iterate over non-null values only (slow path with nulls)" do
      col = Float64Col.new("test", [1.0, nil, 3.0, nil, 5.0] of Float64?)

      values = [] of Float64
      col.each_non_null { |v| values << v }
      values.should eq([1.0, 3.0, 5.0])
    end

    it "should handle nulls in lazy iteration" do
      col = Float64Col.new("test", [1.0, nil, 3.0] of Float64?)

      values = [] of Float64?
      col.each { |v| values << v }
      values.should eq([1.0, nil, 3.0])
    end

    it "should provide unsafe_fetch for direct access" do
      col = Float64Col.new("test", [1.0, nil, 3.0] of Float64?)

      col.unsafe_fetch(0).should eq(1.0)
      col.unsafe_fetch(1).should eq(nil)
      col.unsafe_fetch(2).should eq(3.0)
    end
  end

  describe "Lazy iteration (Int32Col)" do
    it "should iterate lazily" do
      col = Int32Col.new("test", [1, 2, 3, 4, 5] of Int32?)

      sum = 0
      col.each { |v| sum += v.not_nil! }
      sum.should eq(15)
    end

    it "should iterate with index" do
      col = Int32Col.new("test", [10, 20, 30] of Int32?)

      pairs = [] of {Int32?, Int32}
      col.each_with_index { |v, i| pairs << {v, i} }

      pairs.should eq([{10, 0}, {20, 1}, {30, 2}])
    end

    it "should iterate over non-null values only" do
      col = Int32Col.new("test", [1, nil, 3, nil, 5] of Int32?)

      values = [] of Int32
      col.each_non_null { |v| values << v }
      values.should eq([1, 3, 5])
    end

    it "should provide unsafe_fetch" do
      col = Int32Col.new("test", [1, nil, 3] of Int32?)

      col.unsafe_fetch(0).should eq(1)
      col.unsafe_fetch(1).should eq(nil)
      col.unsafe_fetch(2).should eq(3)
    end
  end

  describe "Lazy iteration (Int64Col)" do
    it "should iterate lazily" do
      col = Int64Col.new("test", [1_i64, 2_i64, 3_i64] of Int64?)

      sum = 0_i64
      col.each { |v| sum += v.not_nil! }
      sum.should eq(6_i64)
    end

    it "should iterate over non-null values only" do
      col = Int64Col.new("test", [1_i64, nil, 3_i64] of Int64?)

      values = [] of Int64
      col.each_non_null { |v| values << v }
      values.should eq([1_i64, 3_i64])
    end

    it "should provide unsafe_fetch" do
      col = Int64Col.new("test", [100_i64, nil, 300_i64] of Int64?)

      col.unsafe_fetch(0).should eq(100_i64)
      col.unsafe_fetch(1).should eq(nil)
      col.unsafe_fetch(2).should eq(300_i64)
    end
  end

  describe "Lazy iteration (StringCol)" do
    it "should iterate lazily" do
      col = StringCol.new("test", ["a", "b", "c"] of String?)

      values = [] of String?
      col.each { |v| values << v }
      values.should eq(["a", "b", "c"])
    end

    it "should iterate with index" do
      col = StringCol.new("test", ["x", "y", "z"] of String?)

      pairs = [] of {String?, Int32}
      col.each_with_index { |v, i| pairs << {v, i} }

      pairs.should eq([{"x", 0}, {"y", 1}, {"z", 2}])
    end

    it "should iterate over non-null values only" do
      col = StringCol.new("test", ["a", nil, "c", nil, "e"] of String?)

      values = [] of String
      col.each_non_null { |v| values << v }
      values.should eq(["a", "c", "e"])
    end

    it "should provide unsafe_fetch" do
      col = StringCol.new("test", ["hello", nil, "world"] of String?)

      col.unsafe_fetch(0).should eq("hello")
      col.unsafe_fetch(1).should eq(nil)
      col.unsafe_fetch(2).should eq("world")
    end
  end

  describe "Lazy iteration performance benefit" do
    it "should allow early termination without full materialization" do
      # Create a large column
      data = Array(Float64?).new(10000) { |i| i.to_f64 }
      col = Float64Col.new("large", data)

      # Find first value > 100 using lazy iteration
      found : Float64? = nil
      col.each do |v|
        if v.not_nil! > 100.0
          found = v
          break
        end
      end

      found.should eq(101.0)
      # The key benefit: we didn't need to materialize the full 10000-element array
    end

    it "should compute sum without array allocation via each_non_null" do
      data = Array(Float64?).new(1000) { |i| i % 3 == 0 ? nil : i.to_f64 }
      col = Float64Col.new("test", data)

      # Sum using lazy iteration
      sum = 0.0
      col.each_non_null { |v| sum += v }

      # Verify against opt_sum
      sum.should eq(col.opt_sum(remove_na: true))
    end
  end
end
