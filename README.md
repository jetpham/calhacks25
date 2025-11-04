# CalHacks Database Query Optimizer

A Rust-based query execution optimized for a silly dataset
> built on DuckDB

## Quick Start

### Run Queries on Judges laptops

```bash
# First run: prepares database in-memory and saves to disk
./calhacks --run --runs 10 --input-dir data/data --output-dir results/benchmark-full --baseline-dir results/results-full

# Subsequent runs with same data: uses cached duck.db file
# (automatically created and reused, no need to specify)
```

## Command Line Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `--run` | **Required flag** to execute queries | `--run` |
| `--output-dir DIR` | Where to save query result CSV files | `--output-dir results/test` |
| `--input-dir DIR` | Directory containing CSV input files | `--input-dir data/data-small` |
| `--baseline-dir DIR` | Compare results against baseline directory | `--baseline-dir results/baseline` |
| `--runs N` | Number of times to run each query (for averaging) | `--runs 5` |
| `--queries FILE` | JSON file with query definitions | `--queries my_queries.json` |
| `--profile` | Enable query profiling | `--profile` |
| `--skip-save` | Skip saving the database to disk (keep in-memory only) | `--skip-save` |
| `--use-existing` | Use an existing numbered database if available | `--use-existing` |

## Features

- **Optimized Data Types**: Uses UUID for auction IDs, USMALLINT for IDs, reducing storage by ~450MB on 15M row datasets
- **Materialized Tables**: Converts views to persistent tables for 70-249x performance improvement
- **Selective Indexing**: Creates indexes only on low-cardinality columns to stay within 16GB memory limits
- **Rollup Tables**: Pre-computes 46 common aggregations for instant lookups
- **Query Profiling**: Built-in EXPLAIN ANALYZE support for performance analysis

## Architecture

```text
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

## Building on Mac (M2)

### Recommended: Build natively on the Mac

Cross-compiling with DuckDB's dynamic library from Linux is complex. Build directly on the Mac:

```bash
# On the Mac itself (install Rust first if needed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# Navigate to project and build
cargo build --release

# Binary will be at: ./target/release/calhacks
```

### Alternative: Cross-compile from Linux (complex)

If you must cross-compile, you'll need to provide libduckdb for Mac:

```bash
# Add the Mac target
rustup target add aarch64-apple-darwin

# Install cargo-zigbuild
cargo install --locked cargo-zigbuild

# Download and place libduckdb.dylib in the project
# Then build
cargo zigbuild --release --target aarch64-apple-darwin
```

**Note**: Cross-compilation requires manually providing the DuckDB library, which makes native Mac building much simpler.

## Checking Dynamic Dependencies

To see what external libraries are needed:

```bash
# On Mac
otool -L ./target/release/calhacks

# On Linux (for reference)
ldd ./target/release/calhacks
```

**DuckDB dependency**: The binary requires DuckDB to be installed on the Mac. Install it with:

```bash
brew install duckdb
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

### Multiple Runs for Benchmarking

Run queries multiple times and get average timings:

```bash
# Run each query 10 times and show averages
./target/release/calhacks --run --runs 10 --output-dir results/benchmark
```

This will show:

- Individual query averages
- Min/max times for each query
- Overall average time

### Compare Results

Compare results against a baseline:

```bash
./target/release/calhacks \
  --run \
  --baseline-dir results/baseline \
  --output-dir results/my-results
```

### Skip Saving Database (In-Memory Only)

If you only need to run queries once without caching the database:

```bash
./target/release/calhacks \
  --run \
  --skip-save \
  --output-dir results/test
```

This is useful for quick tests where you don't need the database cached to disk.

## Command Line Arguments (Detailed)

| Argument | Description | Default |
|----------|-------------|---------|
| `--input-dir DIR` | Directory containing CSV files | `data/data` |
| `--output-dir DIR` | Output directory for query results | Required with `--run` |
| `--queries FILE` | JSON file with query definitions | `queries.json` |
| `--load-db FILE` | Load existing database instead of CSV | None |
| `--baseline-dir DIR` | Compare results against baseline | None |
| `--profile` | Enable EXPLAIN ANALYZE profiling | False |
| `--runs N` | Number of times to run each query (for averaging) | 1 |
| `--skip-save` | Skip saving database to disk | False |
| `--use-existing` | Use existing database file if available | False |

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

### Expected Performance

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

## Building

### Prerequisites

- **Rust**: Install via `brew install rust` or `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- **Cargo**: Comes with Rust

### Build for Mac

```bash
# Build release binary (optimized)
cargo build --release

# The binary will be at: ./target/release/calhacks
```

### Using Nix Flake (Optional)

For reproducible development environment with all dependencies:

```bash
# Enter the development shell with Rust, DuckDB, and all tools
nix develop

# Then build
cargo build --release
```

Or with `direnv` (one-time setup):

```bash
direnv allow
cargo build --release
```

## Troubleshooting

### "Table events_table does not exist"

If loading a pre-built database, ensure `events_table` exists. If it doesn't, the loader will recreate it from the `events` view.

### Memory Errors

If running out of memory:

1. Use smaller dataset (`--input-dir data/data-small`)
2. Reduce number of runs (`--runs 1`)

### Mac-Specific Issues

**Inspecting dynamic dependencies:**

```bash
# Check what libraries the binary needs
otool -L ./target/release/calhacks

# You should see libduckdb.dylib listed
```

**DuckDB linking issues:**

If you get errors about missing DuckDB libraries:

```bash
# Install DuckDB via Homebrew
brew install duckdb

# The binary will look for libduckdb.dylib in:
# - /opt/homebrew/lib (Apple Silicon Mac)
# - /usr/local/lib (Intel Mac)
```

**Libraries that need to be in PATH:**

- `libduckdb.dylib` - Core DuckDB library (from `brew install duckdb`)
- Standard system libraries (automatically found)

**Binary size:**
The release binary is optimized for size and performance. On Mac it should be ~10-15MB after stripping.

**Checking if all dependencies are found:**

```bash
# Run this to see if libraries are missing
DYLD_PRINT_LIBRARIES=1 ./target/release/calhacks --version 2>&1 | grep "library not loaded"
```

## Files

- `src/main.rs` - Entry point and query execution
- `src/data_loader.rs` - CSV loading and type optimization
- `src/preprocessor.rs` - Index and rollup creation
- `src/query_executor.rs` - Query execution and profiling
- `src/query_handler.rs` - JSON query parsing
- `src/result_checker.rs` - Result comparison

## License

MIT
