<div align="left" style="margin-bottom: 20px;">
  <img src="ruckdb.svg" alt="RuckDB Logo" width="135" style="vertical-align: middle; margin-right: 20px;" />
  <h1 style="display: inline-block; vertical-align: middle; margin: 0;">CalHacks Database Query Optimizer</h1>
</div>

My attempt at the CalHacks 2025 query planner challenge using DuckDB and Rust.

## Performance

**Database preprocessing**: 4m 18.9s

Benchmark results on the full dataset (245M rows) with 1000 runs:

| Query / Metric | judges.json | queries.json |
|----------------|-------------|--------------|
| Query 1 | 0.24ms | 0.77ms |
| Query 2 | 0.40ms | 0.90ms |
| Query 3 | 1.63ms | 0.96ms |
| Query 4 | 1.66ms | 1.96ms |
| Query 5 | 1.56ms | 1.74ms |
| Query 6 | 0.19ms | - |
| Query 7 | 0.20ms | - |
| Query 8 | 4.49ms | - |
| Query 9 | 0.88ms | - |
| Query 10 | 0.77ms | - |
| Query 11 | 1.23ms | - |
| Query 12 | 0.86ms | - |
| Query 13 | 2.51ms | - |
| Query 14 | 0.84ms | - |
| Query 15 | 0.75ms | - |
| **Sum of averages** | **18.20ms** | **6.31ms** |
| Query preparation and warmup | 1.4s | 1.3s |
| Total execution time (1000 runs) | 1.8s | 6.5s |

## Usage

### Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--input-dir DIR` | Directory containing CSV files | Required |
| `--output-dir DIR` | Output directory for query results | Required with `--run` |
| `--queries FILE` | JSON file with query definitions | Required |
| `--run` | Execute queries (required flag) | - |
| `--runs N` | Number of times to run each query (for averaging) | 1 |
| `--use-existing FILE` | Use existing database file (specify path) | None |
| `--baseline-dir DIR` | Compare results against baseline | None |
| `--profile` | Enable EXPLAIN ANALYZE profiling | False |

## Building

### Prerequisites

- **Rust**
- **DuckDB**

### Build Release Binary

```bash
# Build optimized release binary
cargo build --release
```

### Using Nix Flake (Optional)

For dev environment:

```bash
# Enter development shell
nix develop

# Build
cargo build --release
```

Or with `direnv`:

```bash
direnv allow
cargo build --release
```

## Quick Start

Create the database and run queries:

```bash
./calhacks \
  --input-dir ../calhacks-applovin-query-planner-challenge/data/data \
  --run \
  --queries ../calhacks-applovin-query-planner-challenge/queries.json \
  --output-dir results/
```
