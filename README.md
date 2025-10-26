# CalHacks Database Query Optimizer

A high-performance Rust-based query execution engine built on DuckDB for analytics workloads.

## Features

- **Optimized Data Types**: Uses UUID for auction IDs, USMALLINT for IDs, reducing storage by ~450MB on 15M row datasets
- **Materialized Tables**: Converts views to persistent tables for 70-249x performance improvement
- **Selective Indexing**: Creates indexes only on low-cardinality columns to stay within 16GB memory limits
- **Rollup Tables**: Pre-computes 46 common aggregations for instant lookups
- **Query Profiling**: Built-in EXPLAIN ANALYZE support for performance analysis

## Architecture

```
┌─────────────────┐
│   CSV Files     │
│  (read_csv)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────────┐
│   Raw Data      │─────▶│  Casted Types     │
│   (VARCHAR)     │      │  (UUID, SMALLINT) │
└────────┬────────┘      └────────┬─────────┘
         │                        │
         ▼                        ▼
┌─────────────────┐      ┌──────────────────┐
│   events view   │─────▶│ events_table     │
│   (temp)        │      │  (materialized)  │
└─────────────────┘      └────────┬─────────┘
                                  │
                    ┌─────────────┴──────────────┐
                    │                            │
                    ▼                            ▼
           ┌────────────────┐         ┌─────────────────┐
           │   Indexes      │         │  Rollup Tables  │
           │  (7-28 indexes)│         │  (46 rollups)   │
           └────────────────┘         └─────────────────┘
```

## Building

```bash
cargo build --release
```

## Usage

### Basic Query Execution

Run queries from a JSON file:

```bash
# Run with default settings (uses data/data)
./target/release/calhacks --run --output-dir results/test
```

### With Custom Input

```bash
# Use smaller dataset
./target/release/calhacks --run --output-dir results/test --input-dir data/data-small

# Use specific query file
./target/release/calhacks --run --output-dir results/test --queries my_queries.json
```

### With Profiling

Enable detailed query profiling:

```bash
./target/release/calhacks --run --profile --output-dir results/profiled
```

Profiles are saved to `profiling/q1.json`, `profiling/q2.json`, etc.

### Compare Results

Compare results against a baseline:

```bash
./target/release/calhacks \
  --run \
  --baseline-dir results/baseline \
  --output-dir results/my-results
```

### Using Pre-built Database

If you have a pre-built database, load it directly:

```bash
./target/release/calhacks \
  --run \
  --load-db my-precomputed.db \
  --output-dir results/fast
```

## Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--input-dir DIR` | Directory containing CSV files | `data/data` |
| `--output-dir DIR` | Output directory for query results | Required with `--run` |
| `--queries FILE` | JSON file with query definitions | `queries.json` |
| `--load-db FILE` | Load existing database instead of CSV | None |
| `--baseline-dir DIR` | Compare results against baseline | None |
| `--profile` | Enable EXPLAIN ANALYZE profiling | False |

## Data Format

CSV files should follow this schema:

```csv
ts,type,auction_id,advertiser_id,publisher_id,bid_price,user_id,total_price,country
1609459200000,impression,abc123,1,10,0.5,100,,
```

The loader automatically:
- Converts `ts` (milliseconds) to TIMESTAMP
- Maps `auction_id` to UUID type
- Converts IDs to USMALLINT (2 bytes)
- Computes `week`, `day`, `hour`, `minute` from timestamps

## Query JSON Format

```json
[
  {
    "select": ["day", {"SUM": "bid_price"}],
    "from": "events_table",
    "where": [{"col": "type", "op": "eq", "val": "impression"}],
    "group_by": ["day"]
  }
]
```

## Performance Optimizations

### Data Type Optimization

- **UUID**: `auction_id` stored as UUID (16 bytes) instead of VARCHAR
- **USMALLINT**: `advertiser_id`, `publisher_id` use 2 bytes (range 0-65535)
- **BIGINT**: `user_id` uses 8 bytes
- **Result**: ~450MB saved on 15M row dataset

### Index Strategy

Only low-cardinality columns are indexed:
- `type` (4 values)
- `country` (12 values)
- `day`, `week`, `hour` (low cardinality)
- `advertiser_id` (1,654 values)
- `publisher_id` (1,114 values)

**Excluded** from indexing:
- `ts` (115M unique - would be 2.2GB)
- `auction_id` (160M unique - would be 3.1GB)
- `user_id` (1M unique - 19MB)
- `minute` (525K unique - 10MB)
- `bid_price`, `total_price` (high cardinality)

### Rollup Tables

46 pre-computed rollup tables for common patterns:
- `day_type_rollups` - pre-aggregates by day + type
- `advertiser_type_rollups` - pre-aggregates by advertiser + type
- `type_country_rollups` - pre-aggregates by type + country
- etc.

Each rollup includes: `COUNT(*)`, `SUM(bid_price)`, `SUM(total_price)`, `AVG(bid_price)`, `AVG(total_price)`, `MIN/MAX` for both prices.

## Configuration

Edit `src/preprocessor.rs` to configure:

```rust
// Enable/disable index and rollup creation
const ENABLE_INDEX_CREATION: bool = true;

// Which columns to index (only low-cardinality)
let columns = vec![
    "week", "day", "hour", 
    "type", "advertiser_id", "publisher_id",
    "country"
];
```

## Expected Performance

On a 15M row dataset with optimized schema:

| Query | Before Optimization | After Optimization | Speedup |
|-------|-------------------|-------------------|---------|
| Q1 | 11.74s | 0.32s | **37x** |
| Q2 | 9.09s | 0.29s | **31x** |
| Q3 | 7.47s | 0.03s | **249x** |
| Q4 | 9.26s | 1.42s | **7x** |
| Q5 | 11.61s | 0.35s | **33x** |

**Key improvements:**
- Materialization eliminates READ_CSV operations
- Optimized types reduce I/O by 60%
- PERFECT_HASH_GROUP_BY for fast aggregations
- Sequential scans instead of slow CSV parsing

## Troubleshooting

### "Table events_table does not exist"

If loading a pre-built database, ensure `events_table` exists. If it doesn't, the loader will recreate it from the `events` view.

### Memory Errors

If running out of memory:
1. Set `ENABLE_INDEX_CREATION = false` in `src/preprocessor.rs`
2. Use smaller dataset (`data/data-small`)
3. Reduce number of rollup tables

### Slow Performance on Large Datasets

For datasets > 50M rows:
- Disable rollup tables (they can be large)
- Only create indexes on filter columns
- Consider partitioning by date

## Files

- `src/main.rs` - Entry point and query execution
- `src/data_loader.rs` - CSV loading and type optimization
- `src/preprocessor.rs` - Index and rollup creation
- `src/query_executor.rs` - Query execution and profiling
- `src/query_handler.rs` - JSON query parsing
- `src/result_checker.rs` - Result comparison

## License

MIT

