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
