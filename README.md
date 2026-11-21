# Shopping Sessions

Go submission for the Databricks Shopping Sessions track. The binary reads the mounted clickstream data and writes `user_daily/` and `campaign_daily/` as Parquet. [Challenge rules](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md?inline=1)

## Run

```sh
make run-container DATA=/path/to/data OUT=/path/to/output
```

The Makefile targets `linux/amd64` and packages a statically linked binary in a scratch image. The image runs as UID 1000, reads `DATA_DIR` (default `/data`), and writes to `OUTPUT_DIR` (default `/output`). It needs no network or scratch files at runtime. The local Makefile uses your UID for bind-mount permissions.

## Verify

Download and unpack the official DuckDB starter, generator, and dev answer. [Track downloads](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)

Build the generator with `cargo build --release --locked` in the generator folder and add its `target/release` directory to `PATH`. Install `polars` in the Python environment used by the starter's checkers. Run the checks on macOS or Linux with Go 1.22 or newer.

```sh
make check-fixtures STARTER=/path/to/starter-duckdb
make dev
make run-dev
make check-dev STARTER=/path/to/starter-duckdb ANSWER=/path/to/dev-answer
```

The official checkers passed all 36 fixtures and matched both Parquet schemas and all rows on the one-million-event dev set. The dev set also matched when the `linux/amd64` image ran offline with a read-only root filesystem and UID 1000.

## Local benchmark

Measured on an Apple M1 Max with eight workers using data from the official generator. Native wall time covers binary launch through Parquet output. Container wall time includes startup in a local Linux VM running the amd64 image through Rosetta.

| Seed | Events | Clicks | Native wall time | Native peak RSS | Container wall time |
| --- | ---: | ---: | ---: | ---: | ---: |
| 22 | 20 million | 5 million | 2.85 s | 2.33 GiB | 5.44 s |
| 42 | 60 million | 15 million | 10.48 s | 7.19 GiB | 21.08 s |

## Pipeline

- Union-find resolves transitive links and retains the bytewise smallest identifier for each person.
- Workers scan newline-aligned chunks and partition event candidates by exact ID. The deduplication table retains the earliest copy even when that copy fails validation.
- Workers sort surviving events and clicks within person partitions. They walk session starts and maintain the last eligible click plus each campaign's active span, so repeated clicks contribute one assist per campaign per session.
- Each worker streams user-day rows into Parquet. Campaign-day totals are merged across the workers.

Production dependencies provide Parquet serialization and read-only memory mapping. [parquet-go](https://github.com/parquet-go/parquet-go) · [golang.org/x/sys](https://pkg.go.dev/golang.org/x/sys)

The submission code is released under the MIT License.
