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
go test ./...
```

The official checkers passed all 36 fixtures and matched both Parquet schemas and all rows on the one-million-event dev set. The dev set also matched when the `linux/amd64` image ran offline with a read-only root filesystem and UID 1000.

## Local benchmark

Both images ran with eight pinned vCPUs and the same read-only inputs from the official generator. Wall time includes startup in a local Linux VM running amd64 through Rosetta on an Apple M1 Max. The table reports medians from three interleaved trials per image at 20 million events and two per image at 60 million events.

| Seed | Events | Clicks | Previous wall time (`f81e629`) | Optimized wall time | Optimized peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| 22 | 20 million | 5 million | 2.32 s | 1.99 s | 2.23 GiB |
| 42 | 60 million | 15 million | 8.12 s | 7.45 s | 6.80 GiB |

Both Parquet tables matched the previous version row for row on each dataset.

## Pipeline

- Workers scan newline-aligned chunks with an AVX2 delimiter scanner on supported CPUs and a portable word-at-a-time scanner elsewhere. Each worker caches validated calendar prefixes. Union-find builds the identity lookup table while the event scan runs.
- Workers partition 32-byte event candidates by exact ID. Wide timestamps and large category IDs use a separate payload. Fingerprints filter hash probes before full ID checks, and the winner is the earliest copy even when that copy fails validation. Click scanning overlaps this stage.
- Counting and prefix sums group surviving rows by person. Workers reuse the person index from the counting pass during scatter. They sort each person's history by timestamp, using radix sort for large event histories. A rolling click window tracks the crediting click and distinct assisting campaigns.
- Each worker streams user-day rows into Parquet. Campaign-day totals are merged across the workers.

Production dependencies provide Parquet serialization and read-only memory mapping. [parquet-go](https://github.com/parquet-go/parquet-go) · [golang.org/x/sys](https://pkg.go.dev/golang.org/x/sys)

The submission code is released under the MIT License.
