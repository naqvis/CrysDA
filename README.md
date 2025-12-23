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
- **Database** - Direct SQL query results to DataFrame

### Column Types

- Numeric: `Int32Col`, `Int64Col`, `Float64Col`
- Text: `StringCol` with automatic interning for categorical data
- Boolean: `BoolCol`
- Nested: `DFCol` for hierarchical data

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

# From code
df = Crysda.dataframe_of("name", "age", "score").values(
  "Alice", 30, 95.5,
  "Bob",   25, 87.0,
  "Carol", 35, 92.3
)
```

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
