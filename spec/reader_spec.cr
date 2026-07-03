require "./spec_helper"
require "http/server"
require "sqlite3"

module Crysda
  describe "Reader" do
    it "skip lines and read file withtout header" do
      filename = "./spec/data/headerless_with_preamble.txt"
      predictions = read_csv(filename, separator: '\t', header: nil, skip: 6)

      predictions.tap do |df|
        df.num_row.should eq(14)
        df.names.should eq(["Col1", "Col2", "Col3"])
        df.head.print
      end
    end

    it "skip comment chars and read file withtout header" do
      filename = "./spec/data/headerless_with_preamble.txt"
      predictions = read_csv(filename, separator: '\t', header: nil, comment: '#')

      predictions.tap do |df|
        df.num_row.should eq(14)
        df.names.should eq(["Col1", "Col2", "Col3"])
        df.head.print
      end
    end

    it "read tornado data file" do
      filename = "./spec/data/1950-2014_torn.csv"
      tornado = read_csv(filename)
      tornado.print
      tornado.schema
    end
  end

  it "should have correct column types" do
    read_csv("./spec/data/test_header_types.csv").tap do |df|
      df.print
      df.schema
      df.cols[0].is_a?(StringCol).should be_true
      df.cols[1].is_a?(StringCol).should be_true
      df.cols[2].is_a?(Float64Col).should be_true
      df.cols[3].is_a?(Int32Col).should be_true
      df.cols[4].is_a?(BoolCol).should be_true
      df.cols[5].is_a?(Int64Col).should be_true
    end
  end

  it "detects BigDecimal from CSV when Float64 loses precision" do
    csv = IO::Memory.new("id,value\n1,3.14159265358979323846\n2,12345678901234567890\n3,0.0000000000001\n")
    df = read_csv(csv)
    df["value"].is_a?(BigDecimalCol).should be_true
    df["value"][0].should eq(BigDecimal.new("3.14159265358979323846"))
  end

  it "keeps Float64 for values that round-trip safely" do
    csv = IO::Memory.new("x\n3.14\n0.1\n1.5\n")
    df = read_csv(csv)
    df["x"].is_a?(Float64Col).should be_true
  end

  it "should read a file with custom NA value" do
    read_csv("./spec/data/custom_na_value.csv", na_value: "CUSTOM_NA").tap do |df|
      df.print
      df.schema
      df.cols[0][0].should eq(nil)
      df.cols[0].is_a?(Int32Col).should be_true
    end
  end

  it "should read compressed gzip file" do
    read_csv("./spec/data/msleep.csv.gz").tap do |df|
      df.print
    end
  end

  it "auto-detects DateTimeCol vs TimestampCol on CSV import" do
    # Epoch seconds → DateTimeCol
    df = read_csv(IO::Memory.new("ts,val\n1690992000,1\n1728777600,2\n"))
    df["ts"].is_a?(DateTimeCol).should be_true

    # Epoch milliseconds → DateTimeCol
    df2 = read_csv(IO::Memory.new("ts,val\n1690992000123,1\n1728777600456,2\n"))
    df2["ts"].is_a?(DateTimeCol).should be_true

    # Epoch nanoseconds → TimestampCol
    df3 = read_csv(IO::Memory.new("ts,val\n1690992000123456789,1\n1728777600987654321,2\n"))
    df3["ts"].is_a?(TimestampCol).should be_true

    # ISO datetime with ms → DateTimeCol
    df4 = read_csv(IO::Memory.new("ts,val\n2023-01-15 10:30:45.123,1\n2023-06-20 14:45:30.987,2\n"))
    df4["ts"].is_a?(DateTimeCol).should be_true

    # ISO datetime with ns → TimestampCol
    df5 = read_csv(IO::Memory.new("ts,val\n2023-01-15 10:30:45.123456789,1\n2023-06-20 14:45:30.987654321,2\n"))
    df5["ts"].is_a?(TimestampCol).should be_true
  end

  it "should read a compressed file from URL" do
    url = "http://localhost:8000/msleep.csv.gz"
    server = HTTP::Server.new([HTTP::StaticFileHandler.new("./spec/data/")])
    server.bind_tcp 8000
    spawn do
      server.listen
    end
    begin
      Fiber.yield
      read_csv(url).tap do |df|
        df.print
      end
    ensure
      server.close
    end
  end

  it "should read a uncompressed file from URL" do
    url = "http://localhost:8000/iris.txt"
    server = HTTP::Server.new([HTTP::StaticFileHandler.new("./spec/data/")])
    server.bind_tcp 8000
    spawn do
      server.listen
    end
    begin
      Fiber.yield
      read_csv(url, separator: '\t').tap do |df|
        df.print
      end
    ensure
      server.close
    end
  end

  it "should read json data from url" do
    url = "http://localhost:8000/movies.json"
    server = HTTP::Server.new([HTTP::StaticFileHandler.new("./spec/data/")])
    server.bind_tcp 8000
    spawn do
      server.listen
    end
    begin
      Fiber.yield
      read_json(url).tap do |df|
        df.print
        df.num_row.should eq(3201)
        df.names.last?.should eq("IMDB Votes")
      end
    ensure
      server.close
    end
  end

  it "should parse json data from string" do
    json = <<-JS
    {
                    "cars": {
                        "Nissan": [
                            {"model":"Sentra", "doors":4},
                            {"model":"Maxima", "doors":4},
                            {"model":"Skyline", "doors":2}
                        ],
                        "Ford": [
                            {"model":"Taurus", "doors":4},
                            {"model":"Escort", "doors":4, "seats":5}
                        ]
                    }
                }
    JS
    from_json(json).tap do |df|
      df.schema
      df.print
      df.num_row.should eq(5)
      df.names.should eq(["_id", "cars", "model", "doors", "seats"])
    end
  end

  it "should read incomplete json data from string" do
    json = <<-JS
    {
      "Nissan": [
               {"model":"Sentra", "doors":4},
               {"model":"Maxima", "doors":4},
               {"model":"Skyline", "seats":9}
           ]
   }
JS
    from_json(json).tap do |df|
      df.schema
      df.print
      df.num_row.should eq(3)
      df.names.should eq(["_id", "model", "seats", "doors"])
    end
  end

  it "should read database resultset" do
    DB.open "sqlite3::memory:" do |db|
      db.exec "create table contacts (name text, age integer, foo number)"
      db.exec "insert into contacts values (?, ?, ?)", "John Doe", 30, 1.5
      db.exec "insert into contacts values (?, ?, ?)", "Sarah", 33, 9.1

      df = db.query "select * from contacts" do |rs|
        Crysda.from(rs)
      end
      df.print
      df.schema
      df.num_row.should eq(2)
      df.num_col.should eq(3)
      df.names.should eq(["name", "age", "foo"])
      df["name"].is_a?(StringCol).should be_true
      df["age"].is_a?(Int64Col).should be_true
      df["foo"].is_a?(Float64Col).should be_true
    end
  end

  it "should read database resultset and handle null" do
    DB.open "sqlite3::memory:" do |db|
      db.exec "create table contacts (name text, age integer, foo number)"
      db.exec "insert into contacts values (?, ?, ?)", "John Doe", 30, nil
      db.exec "insert into contacts values (?, ?, ?)", nil, 33, 9.1
      db.exec "insert into contacts values (?, ?, ?)", "Sarah", nil, 7.5

      df = db.query "select * from contacts" do |rs|
        Crysda.from(rs)
      end
      df.print
      df.schema
      df.num_row.should eq(3)
      df.num_col.should eq(3)
      df.names.should eq(["name", "age", "foo"])
      df["name"].is_a?(StringCol).should be_true
      df["age"].is_a?(Int64Col).should be_true
      df["foo"].is_a?(Float64Col).should be_true
    end
  end

  describe "converter registry" do
    it "registers and retrieves a converter" do
      Crysda.register_converter("DuckDB::Timestamp") { |s| Time.parse(s, "%Y-%m-%d %H:%M:%S", Time::Location::UTC) }
      converter = Crysda.converter_for("DuckDB::Timestamp")
      converter.should_not be_nil
      result = converter.try &.call("2023-01-15 10:30:45")
      result.should be_a(Time)
      result.as(Time).year.should eq(2023)
    end

    it "returns nil for unregistered type" do
      Crysda.converter_for("NonExistent::Type").should be_nil
    end

    it "registers multiple converters" do
      Crysda.register_converter("Foo::Bar") { |s| s.to_s.to_i32 }
      Crysda.register_converter("Baz::Qux") { |s| s.to_s.to_f64 }
      Crysda.converter_for("Foo::Bar").should_not be_nil
      Crysda.converter_for("Baz::Qux").should_not be_nil
    end
  end
end
