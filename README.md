# CalHacks Database Query Optimizer

Jet's attempt at the CalHacks 2025 query planner challenge using DuckDB and Rust.

## Performance

### Benchmark Results: judges.json (1000 runs)

Benchmark results on the full dataset (245M rows) with `judges.json` queries (1000 runs):

```text
Using existing database: duck4.db
Query preparation and warmup completed in 2.0s
Query execution completed in 21.7s

=== Query Performance Summary ===
Query 1: 0.27ms average
Query 2: 0.44ms average
Query 3: 2.10ms average
Query 4: 1.90ms average
Query 5: 1.86ms average
Query 6: 0.22ms average
Query 7: 0.23ms average
Query 8: 5.21ms average
Query 9: 1.00ms average
Query 10: 0.92ms average
Query 11: 1.41ms average
Query 12: 0.96ms average
Query 13: 3.03ms average
Query 14: 0.97ms average
Query 15: 0.87ms average
Sum of averages: 21.40ms
```

**Performance:**

- **Query preparation and warmup**: 2.0s
- **Total execution time (1000 runs)**: 21.7s
- **Sum of query averages**: 21.40ms

### Benchmark Results: queries.json (1000 runs)

Benchmark results on the full dataset with `queries.json` queries (1000 runs):

```text
Using existing database: duck4.db
Query preparation and warmup completed in 1.3s
Query execution completed in 6.5s

=== Query Performance Summary ===
Query 1: 0.77ms average
Query 2: 0.90ms average
Query 3: 0.96ms average
Query 4: 1.96ms average
Query 5: 1.74ms average
Sum of averages: 6.31ms
```

**Performance:**

- **Query preparation and warmup**: 1.3s
- **Total execution time (1000 runs)**: 6.5s
- **Sum of query averages**: 6.31ms

## Quick Start

### First Run (Create Database)

```bash
# Build the release binary
cargo build --release

# Create database from CSV files and run queries
./target/release/calhacks \
  --input-dir ../calhacks-applovin-query-planner-challenge/data/data \
  --run \
  --queries ../calhacks-applovin-query-planner-challenge/queries.json \
  --output-dir results/benchmark-full-queries
```

### Subsequent Runs (Use Existing Database)

```bash
# Uses cached database (much faster)
# Specify the database file to use
cargo run --release -- \
  --input-dir ../calhacks-applovin-query-planner-challenge/data/data \
  --use-existing duck4.db \
  --run \
  --queries ../calhacks-applovin-query-planner-challenge/judges.json \
  --output-dir results/benchmark-full-judges
```

### Benchmark with Multiple Runs

```bash
# Run queries 1000 times and get average timings
cargo run --release -- \
  --input-dir ../calhacks-applovin-query-planner-challenge/data/data \
  --use-existing duck4.db \
  --run \
  --queries ../calhacks-applovin-query-planner-challenge/judges.json \
  --output-dir results/benchmark \
  --runs 1000 \
  --profile
```

## Usage

### Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--input-dir DIR` | Directory containing CSV files | `data/data` |
| `--output-dir DIR` | Output directory for query results | Required with `--run` |
| `--queries FILE` | JSON file with query definitions | `queries.json` |
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
