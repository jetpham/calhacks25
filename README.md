# CalHacks Database Query Optimizer

Jet's attempt at the CalHacks 2025 query planner challenge using DuckDB and Rust.

## Performance

### Benchmark Results: judges.json (100 runs)

Benchmark results on the full dataset (245M rows) with `judges.json` queries (100 runs):

| Query | Average | Min | Max |
|-------|---------|-----|-----|
| Q1  | 0.000s | 0.000s | 0.001s |
| Q2  | 0.000s | 0.000s | 0.001s |
| Q3  | 0.006s | 0.005s | 0.009s |
| Q4  | 0.012s | 0.010s | 0.015s |
| Q5  | 0.012s | 0.009s | 0.015s |
| Q6  | 0.000s | 0.000s | 0.001s |
| Q7  | 0.000s | 0.000s | 0.001s |
| Q8  | 0.001s | 0.000s | 0.002s |
| Q9  | 0.003s | 0.002s | 0.004s |
| Q10 | 0.003s | 0.002s | 0.005s |
| Q11 | 0.021s | 0.019s | 0.024s |
| Q12 | 0.004s | 0.003s | 0.007s |
| Q13 | 0.001s | 0.001s | 0.001s |
| Q14 | 0.003s | 0.002s | 0.004s |
| Q15 | 0.023s | 0.021s | 0.028s |

**Performance:**

- **Average total time per run**: 0.090s
- **Sum of all minimum times**: 0.074s

### Benchmark Results: queries.json (100 runs)

Benchmark results on the full dataset with `queries.json` queries (100 runs):

| Query | Average | Min | Max |
|-------|---------|-----|-----|
| Q1 | 0.000s | 0.000s | 0.001s |
| Q2 | 0.040s | 0.036s | 0.049s |
| Q3 | 0.004s | 0.003s | 0.007s |
| Q4 | 0.006s | 0.004s | 0.008s |
| Q5 | 0.012s | 0.010s | 0.017s |

**Performance:**

- **Average total time per run**: 0.062s
- **Sum of all minimum times**: 0.053s

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
./target/release/calhacks \
  --input-dir ../calhacks-applovin-query-planner-challenge/data/data \
  --run \
  --queries ../calhacks-applovin-query-planner-challenge/judges.json \
  --output-dir results/benchmark-full-judges \
  --use-existing
```

### Benchmark with Multiple Runs

```bash
# Run queries 100 times and get average timings
./target/release/calhacks \
  --input-dir ../calhacks-applovin-query-planner-challenge/data/data \
  --run \
  --queries ../calhacks-applovin-query-planner-challenge/judges.json \
  --output-dir results/benchmark \
  --use-existing \
  --runs 100
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
| `--use-existing` | Use existing database file if available | False |
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
