# CrysDA

`CrysDA` is a **Crys**tal shard for **D**ata **A**nalysis. Provides a modern functional-style API for data manipulation to filter, transform, aggregate and reshape tabular data.

Inspired by R's [`dplyr`](https://github.com/hadley/dplyr) and Python's [`pandas`](http://pandas.pydata.org/), written in pure Crystal with no external dependencies.

## Installation

Add to your `shard.yml`:

```yaml
dependencies:
  crysda:
    github: naqvis/CrysDA
```

Then run `shards install`.

## Quick Example

```crystal
require "crysda"

flights = Crysda.read_csv("./data/nycflights.tsv.gz", separator: '\t')

flights
  .group_by("year", "month", "day")
  .summarize(
    "mean_arr_delay".with { |s| s["arr_delay"].mean(remove_na: true) },
    "mean_dep_delay".with { |s| s["dep_delay"].mean(true) })
  .filter { |f| (f["mean_arr_delay"] > 30).or(f["mean_dep_delay"] > 30) }
  .print
```

```
A DataFrame: 49 x 5
     year   month   day   mean_arr_delay   mean_dep_delay
 1   2013       1    16           34.247           24.613
 2   2013       1    31           32.603           28.658
 3   2013      10     7           39.017           39.147
...
```

## Features

### Data Operations

- **Filter, select, mutate** - Subset and transform data with expressive syntax
- **Group & summarize** - Aggregations with `mean`, `sum`, `min`, `max`, `median`, `sd`
- **Joins** - Left, right, inner, outer joins with single or multiple keys
- **Reshape** - `gather`/`spread` for wide↔long transformations, `separate`/`unite` for column splitting

### I/O Support

- **CSV/TSV** - Plain or compressed (gzip), local or remote URLs
- **JSON** - Array of objects format
- **Database** - Direct SQL query results to DataFrame (with extension API for custom DB types)

### Column Types

- Numeric: `Int32Col`, `Int64Col`, `Float64Col`, `BigDecimalCol`
- Text: `StringCol` with automatic interning for categorical data
- Boolean: `BoolCol`
- DateTime: `DateTimeCol` (millisecond precision) with component extraction and formatting
- Timestamp: `TimestampCol` (nanosecond precision) for high-resolution time data
- Nested: `DFCol` for hierarchical data

Numeric types are auto-detected on CSV import:
- Integer values → `Int32Col` or `Int64Col`
- Decimal values → `Float64Col` (unless Float64 loses precision, then `BigDecimalCol`)
- `BigDecimalCol` preserves arbitrary-precision decimal values from CSV or DB sources

### Missing Data

```crystal
df.dropna                      # Remove rows with any nulls
df.dropna("col1", "col2")      # Check only specific columns
df.fillna(0)                   # Fill all nulls with value
df.fillna({"age" => 0, "name" => "Unknown"})  # Column-specific fills
```

### String Operations

```crystal
# No casting needed - use str_ prefix methods
df["name"].str_contains("john")        # Check substring
df["name"].str_contains(/\d+/)         # Regex match
df["name"].str_starts_with("Dr.")      # Prefix check
df["name"].str_ends_with("Jr.")        # Suffix check
df["name"].str_extract(/(\d+)/)        # Extract regex groups
df["name"].str_replace("old", "new")   # Substitute patterns
df["name"].str_upcase                  # Convert to uppercase
df["name"].str_downcase                # Convert to lowercase
df["name"].str_strip                   # Trim whitespace
df["name"].str_len                     # String lengths

# Use in filters
df.filter { |e| e["email"].str_contains("@gmail.com") }

# Or use the block-based matching for simple cases
df.filter { |e| e["name"].matching { |s| s.starts_with?("A") } }
```

### Window Functions

```crystal
col = df["price"].as(Float64Col)
col.rolling_mean(3)            # 3-period moving average
col.rolling_sum(5)             # 5-period moving sum
col.rolling_min(3)             # Moving minimum
col.rolling_max(3)             # Moving maximum
col.rolling_std(3)             # Moving standard deviation
col.ewm_mean(10)               # Exponential weighted moving average
col.diff                       # Difference from previous value
```

### Convenience APIs

Column operations:

```crystal
df["age"].unique                   # Distinct non-null values
df["age"].nunique                  # Count of unique values
df["age"].in?([25, 30, 35])        # Membership check
df["age"].between(20, 40)          # Range check (inclusive)
df["score"].clip(0, 100)           # Clamp to range
df["value"].ffill                  # Forward fill nulls
df["value"].bfill                  # Backward fill nulls
df["value"].first                  # First non-null value
df["value"].last                   # Last non-null value
df["price"].apply { |v| v.as(Float64) * 2 }  # Transform values
df["col1"].coalesce(df["col2"])    # First non-null from two columns

# Binning/Discretization
df["age"].cut([0, 18, 35, 65, 100], labels: ["child", "young", "adult", "senior"])
df["score"].qcut(4)                # Quartile-based binning
df["score"].qcut(4, labels: ["Q1", "Q2", "Q3", "Q4"])
```

DataFrame operations:

```crystal
df.sample(10)                      # Random n rows
df.sample(0.1)                     # Random fraction
df.shuffle                         # Randomize order
df.value_counts("category")        # Count occurrences
df.describe                        # Summary statistics
df.coalesce("a", "b", "c")         # First non-null across columns
df.duplicated("id")                # Find duplicates
df.drop_duplicates("id")           # Remove duplicates
df.nlargest(10, "sales")           # Top 10 rows by sales
df.nsmallest(5, "price")           # Bottom 5 rows by price
Crysda.concat([df1, df2])          # Vertical stack
Crysda.concat([df1, df2], axis: 1) # Horizontal stack

# Row-wise operations
df.apply_rows("total") { |row| row["price"].as_f * row["qty"].as_i }

# Pivot tables
df.pivot_table("region", "product", "sales", "sum")

# JSON output
df.to_json                         # Convert to JSON string
df.to_json(pretty: true)           # Pretty-printed JSON
df.write_json("output.json")       # Write to file
```

### Performance

CrysDA uses optimized internal storage for efficient memory usage and fast operations:

- **Compact storage** - Numeric columns use contiguous `Slice(T)` with separate null bitmap (1 bit per element) instead of nullable union types
- **SIMD acceleration** - Bitmap operations use SSE (x86_64) or NEON (AArch64) instructions for large datasets
- **Zero-allocation aggregations** - `sum`, `mean`, `min`, `max` operate directly on raw data without intermediate arrays
- **String interning** - Categorical string columns automatically deduplicate repeated values
- **Lazy iteration** - Column iterators avoid materializing full arrays when possible

## Usage

### Reading Data

```crystal
# From CSV (auto-detects types)
df = Crysda.read_csv("data.csv")
df = Crysda.read_csv("http://example.com/data.csv")  # from URL
df = Crysda.read_csv("data.tsv.gz", separator: '\t') # compressed TSV

# From DB result set
DB.open "postgres://localhost/mydb" do |db|
  db.query "SELECT * FROM events" do |rs|
    df = Crysda.from(rs)
  end
end

# From code
df = Crysda.dataframe_of("name", "age", "score").values(
  "Alice", 30, 95.5,
  "Bob",   25, 87.0,
  "Carol", 35, 92.3
)
```

#### Custom DB Types (Converter Registry)

DB drivers may return types not in CrysDA's `Any` union (e.g., `PG::Numeric`). Register a converter to handle them:

```crystal
Crysda.register_converter("PG::Numeric") { |s| s.to_f64 }

DB.open "postgres://localhost/mydb" do |db|
  db.query "SELECT amount FROM transactions" do |rs|
    df = Crysda.from(rs)
  end
end
```

The converter receives the value's string representation (`to_json` if available, fallback `to_s`). Unregistered types auto-convert to `StringCol`.

### Inspecting Data

```crystal
df.print           # Pretty-print table
df.schema          # Show column types
df.num_row         # Row count
df.num_col         # Column count
df.names           # Column names
```

### Selecting & Filtering

```crystal
df.select("name", "score")              # Select columns
df.reject("age")                        # Exclude columns
df.filter { |e| e["age"] > 25 }         # Filter rows
df.filter { |e| e["name"].matching { |s| s.starts_with?("A") } }
```

### Transforming

```crystal
df.add_column("score_pct") { |e| e["score"] / 100.0 }
df.add_column("label") { "student" }
df.sort_by("age")
df.sort_desc_by("score")
```

### Aggregating

```crystal
df.summarize(
  "avg_score".with { |e| e["score"].mean },
  "max_age".with { |e| e["age"].max }
)

df.group_by("department").summarize(
  "count".with { |e| e.num_row },
  "avg_score".with { |e| e["score"].mean(remove_na: true) }
)

df.count("department")  # Cross-tabulation
```

### Joining

```crystal
left.left_join(right, "id")
left.inner_join(right, by: ["id", "year"])
left.outer_join(right, by: "key", suffices: {"_l", "_r"})
```

### Reshaping

```crystal
# Wide to long
df.gather("quarter", "value", selector { |c| c["q1".."q4"] })

# Long to wide
df.spread("quarter", "value")

# Split column
df.separate("date", into: ["year", "month", "day"], sep: "-")

# Combine columns
df.unite("full_name", ["first", "last"], sep: " ")
```

## Plotting

For data visualization, see [crysda-plot](https://github.com/naqvis/crysda-plot).

## Documentation

- [API Reference](https://naqvis.github.io/CrysDA/)
- [Tutorial: Getting Started](tutorials/getting-started.md)
- [Tutorial: Reshaping Data](tutorials/reshaping.md)

## Development

```
crystal spec
```

## Contributing

1. Fork it (<https://github.com/naqvis/Crysda/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Ali Naqvi](https://github.com/naqvis) - creator and maintainer
