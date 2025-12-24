# Tutorial: Getting Started with CrysDA

This tutorial walks through common data analysis tasks using CrysDA.

## Loading Data

```crystal
require "crysda"

# From CSV (auto-detects column types)
df = Crysda.read_csv("./data/sales.csv")

# Preview the data
df.print(max_rows: 5)
```

```
A DataFrame: 1000 x 5
    id   region   product   quantity   price
1    1    North   Widget A        10   25.50
2    2    South   Widget B         5   30.00
3    3    North   Widget A         8   25.50
4    4     East   Widget C        12   15.75
5    5    South   Widget A         3   25.50
and 995 more rows
```

## Inspecting Structure

```crystal
df.schema
```

```
DataFrame with 1000 observations
id       [Int32]   1, 2, 3, 4, 5, ...
region   [String]  North, South, North, East, South, ...
product  [String]  Widget A, Widget B, Widget A, Widget C, ...
quantity [Int32]   10, 5, 8, 12, 3, ...
price    [Float64] 25.50, 30.00, 25.50, 15.75, 25.50, ...
```

## Handling Data Quality Issues

Check for unexpected values:

```crystal
df.count("region").print
```

```
    region     n
1    North   250
2    South   248
3     East   251
4     West   249
5  Unknown     2
```

Filter out bad data:

```crystal
clean_df = df.filter { |f| f["region"] != "Unknown" }
```

Or reload with NA handling:

```crystal
df = Crysda.read_csv("./data/sales.csv", na_value: "Unknown")
```

## Adding Computed Columns

```crystal
df = df.add_column("total") { |c| c["quantity"] * c["price"] }

df.schema
```

```
...
total [Float64] 255.00, 150.00, 204.00, 189.00, 76.50, ...
```

## Basic Statistics

```crystal
df.summarize(
  "avg_quantity".with { |c| c["quantity"].mean },
  "total_revenue".with { |c| c["total"].sum }
).print
```

```
    avg_quantity   total_revenue
1           8.5        125000.00
```

Or access directly:

```crystal
puts df["quantity"].mean  # => 8.5
puts df["total"].sum      # => 125000.0
```

## Grouping & Aggregation

Sales by region:

```crystal
df.group_by("region")
  .summarize(
    "orders".with { |e| e.num_row },
    "revenue".with { |e| e["total"].sum }
  )
  .sort_desc_by("revenue")
  .print
```

```
    region   orders   revenue
1    North      250   35000.00
2     East      251   32500.00
3    South      248   31000.00
4     West      249   26500.00
```

Cross-tabulation:

```crystal
df.count("region", "product").print
```

## Filtering with Multiple Conditions

High-value orders from North region:

```crystal
df.filter { |e|
  (e["region"] == "North")
    .and(e["total"] >= 100.0)
}
.select("product", "quantity", "total")
.sort_desc_by("total")
.print
```

## Selecting Columns

```crystal
# Keep specific columns
df.select("region", "product", "total")

# Exclude columns
df.reject("id")

# By pattern
df.select { |c| c.ends_with?("_id") }
```

## Sorting

```crystal
df.sort_by("region", "product")      # Ascending
df.sort_desc_by("total")             # Descending
df.sort_by { |e| e["quantity"] }     # By expression
```

## Method Chaining

CrysDA operations return new DataFrames, enabling fluent chaining:

```crystal
result = df
  .filter { |e| e["quantity"] > 5 }
  .add_column("total") { |e| e["quantity"] * e["price"] }
  .group_by("region")
  .summarize(
    "orders".with { |e| e.num_row },
    "avg_order".with { |e| e["total"].mean }
  )
  .sort_desc_by("avg_order")

result.print
```

## Summary

Key operations:

- `read_csv` - Load data with automatic type inference
- `schema` / `print` - Inspect data structure
- `filter` - Subset rows by condition
- `select` / `reject` - Choose columns
- `add_column` - Create computed columns
- `group_by` + `summarize` - Aggregate data
- `sort_by` / `sort_desc_by` - Order results
- Method chaining for readable pipelines

## Handling Missing Data

CrysDA provides convenient methods for dealing with null values:

```crystal
# Remove rows with any null values
clean_df = df.dropna

# Remove rows with nulls only in specific columns
clean_df = df.dropna("quantity", "price")

# Fill nulls with a default value
filled_df = df.fillna(0)

# Fill with column-specific values
filled_df = df.fillna({"quantity" => 0, "region" => "Unknown"})
```

## String Operations

CrysDA provides convenient `str_` prefixed methods that work directly on columns without casting:

```crystal
names = df["product"]

# Pattern matching
names.str_contains("Widget")           # Check substring
names.str_contains(/^Widget [A-C]$/)   # Regex match
names.str_starts_with("Widget")        # Prefix check
names.str_ends_with("Pro")             # Suffix check

# Extraction and transformation
names.str_extract(/(\d+)/)             # Extract regex groups
names.str_replace("Widget", "Product") # Substitute patterns
names.str_upcase                       # Convert to uppercase
names.str_downcase                     # Convert to lowercase
names.str_strip                        # Trim whitespace

# String properties
names.str_len                          # Get string lengths
```

Use in filters:

```crystal
df.filter { |e| e["product"].str_contains("Widget") }
df.filter { |e| e["email"].str_ends_with("@company.com") }
```

For simple string matching, the block-based `matching` method is also available:

```crystal
df.filter { |e| e["name"].matching { |s| s.starts_with?("A") } }
```

## Window Functions

For time-series and analytical queries, use window functions on numeric columns:

```crystal
prices = df["price"].as(Float64Col)

# Moving averages
prices.rolling_mean(3)             # 3-period moving average
prices.rolling_mean(5, min_periods: 1)  # Allow partial windows

# Other rolling calculations
prices.rolling_sum(3)              # Moving sum
prices.rolling_min(3)              # Moving minimum
prices.rolling_max(3)              # Moving maximum
prices.rolling_std(3)              # Moving standard deviation

# Exponential smoothing
prices.ewm_mean(10)                # Exponential weighted moving average

# Differences
prices.diff                        # Change from previous value
prices.diff(7)                     # Change from 7 periods ago
```

Example: Calculate 7-day moving average of sales:

```crystal
daily_sales
  .sort_by("date")
  .add_column("ma7") { |e| e["revenue"].as(Float64Col).rolling_mean(7, min_periods: 1) }
  .print
```

## DateTime Columns

CrysDA automatically detects datetime columns in CSV files:

```crystal
df = Crysda.read_csv("events.csv")
# Date column is automatically parsed as DateTimeCol

dates = df["date"].as(DateTimeCol)

# Extract components
dates.year                         # Year as Int32Col
dates.month                        # Month (1-12)
dates.day                          # Day of month (1-31)
dates.hour                         # Hour (0-23)
dates.minute                       # Minute (0-59)
dates.day_of_week                  # Day of week (1=Mon, 7=Sun)
dates.day_of_year                  # Day of year (1-366)

# Aggregations
dates.min                          # Earliest date
dates.max                          # Latest date

# Formatting
dates.strftime("%Y-%m-%d")         # Format as strings

# Comparisons
dates > Time.utc(2023, 1, 1)       # Filter by date
```

Example: Group events by month:

```crystal
df.add_column("month") { |e| e["date"].as(DateTimeCol).month }
  .group_by("month")
  .summarize("count".with { |e| e.num_row })
  .print
```

## Convenience APIs

CrysDA provides convenience methods for common data manipulation tasks, inspired by pandas.

### Column Value Inspection

```crystal
df["category"].unique              # => ["A", "B", "C"] - distinct non-null values
df["category"].nunique             # => 3 - count of unique values
```

### Membership & Range Checks

```crystal
# Check if values are in a set - returns Array(Bool) for filtering
df["status"].in?(["active", "pending"])
df["status"].is_in(["active", "pending"])  # alias

# Range check (inclusive) - works on numeric columns
df["age"].between(18, 65)

# Use in filters
df.filter { |e| e["age"].between(25, 35) }
df.filter { |e| e["status"].in?(["active", "pending"]) }
```

### Value Clamping

```crystal
# Constrain values to a range
df["score"].clip(0, 100)           # Values outside [0,100] are clamped
df["score"].clip_lower(0)          # Ensure minimum of 0
df["score"].clip_upper(100)        # Ensure maximum of 100

# Use in add_column
df.add_column("clamped") { |e| e["score"].clip(0, 100) }
```

### Null Filling Strategies

```crystal
# Forward fill - propagate last valid value forward
df["value"].ffill
# [1.0, nil, nil, 4.0, nil] => [1.0, 1.0, 1.0, 4.0, 4.0]

# Backward fill - propagate next valid value backward
df["value"].bfill
# [nil, 2.0, nil, nil, 5.0] => [2.0, 2.0, 5.0, 5.0, 5.0]

# Use in add_column to create filled version
df.add_column("filled") { |e| e["price"].ffill }
```

### Value Transformation

```crystal
# Apply function to each non-null value
df["price"].apply { |v| v.as(Float64) * 1.1 }  # 10% markup
```

### Binning/Discretization

```crystal
# Bin values into discrete intervals (like pandas cut)
df["age"].cut([0, 18, 35, 65, 100], labels: ["child", "young", "adult", "senior"])

# Without labels, generates interval strings like "(0, 18]"
df["age"].cut([0, 18, 35, 65, 100])

# Quantile-based binning (like pandas qcut)
df["score"].qcut(4)                            # Quartiles
df["score"].qcut(4, labels: ["Q1", "Q2", "Q3", "Q4"])
df["income"].qcut(10)                          # Deciles
```

### Column-Level Coalesce

```crystal
# Get first non-null value from two columns
df["primary_phone"].coalesce(df["backup_phone"])
```

### Sampling

```crystal
df.sample(100)                     # Random 100 rows
df.sample(0.1)                     # Random 10% of rows
df.sample(50, seed: 42)            # Reproducible sampling
df.shuffle                         # Randomize all rows
df.shuffle(seed: 42)               # Reproducible shuffle
```

### Value Counts

```crystal
df.value_counts("category")        # Count occurrences, sorted desc
```

```
    category     n
1    Widget A   450
2    Widget B   320
3    Widget C   230
```

### Summary Statistics

```crystal
df.describe                        # Stats for all numeric columns
```

```
    statistic        price     quantity
1       count       1000.0       1000.0
2        mean         25.5          8.5
3         std         10.2          4.3
4         min          5.0          1.0
5         25%         18.0          5.0
6         50%         25.0          8.0
7         75%         32.0         12.0
8         max         50.0         20.0
```

### DataFrame Coalesce

Get first non-null value across multiple columns:

```crystal
df.coalesce("primary_phone", "backup_phone", "emergency_phone")
```

### Duplicate Handling

```crystal
df.duplicated("customer_id")       # Array(Bool) - true for duplicates
df.drop_duplicates("customer_id")  # Keep first occurrence only
df.drop_duplicates("email", "name") # Check multiple columns
```

### Concatenation

```crystal
# Vertical stack (row-wise)
Crysda.concat([df1, df2, df3])

# Horizontal stack (column-wise) - all must have same row count
Crysda.concat([df1, df2], axis: 1)
```

### Row-wise Operations

```crystal
# Apply a function to each row
df.apply_rows("total") { |row| row["price"].as_f * row["qty"].as_i }
df.apply_rows("full_name") { |row| "#{row["first"].as_s} #{row["last"].as_s}" }
```

### Pivot Tables

```crystal
# Reshape data by aggregating values
df.pivot_table("region", "product", "sales", "sum")
df.pivot_table("year", "category", "revenue", "mean")
df.pivot_table(["region", "year"], "product", "sales", "count")
```

Supported aggregation functions: `"sum"`, `"mean"`, `"count"`, `"min"`, `"max"`
