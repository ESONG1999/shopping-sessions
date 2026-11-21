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

Both images ran with eight pinned vCPUs, a 24 GiB container limit, no swap, and read-only inputs from the official generator. Wall time includes startup in a local Linux VM running amd64 through Rosetta on an Apple M1 Max. The warm result is the median of three interleaved runs per image. Cold results are the medians of two paired runs after requesting eviction of the input files from the guest page cache.

| Seed | Events | Clicks | Cache | Previous wall time (`b00ed9f`) | Optimized wall time | Optimized cgroup peak |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| 22 | 20 million | 5 million | Warm | 2.16 s | 2.02 s | 1.66 GiB |
| 22 | 20 million | 5 million | Cold | 3.08 s | 1.97 s | 3.36 GiB |
| 42 | 60 million | 15 million | Cold | 15.06 s | 9.99 s | 9.86 GiB |

Both Parquet tables matched the previous version row for row on each dataset. On the full cold input the optimized image averaged about seven CPU cores. The cgroup charged about 5.41 GiB of file cache, with no memory-limit events. In a separate cold experiment, retaining a second copy of the CSV in anonymous memory increased peak usage from 11.00 to 15.19 GiB and wall time from 10.98 to 16.04 s.

The full run wrote about 62 MiB of Parquet with zero scratch-file bytes and an observed peak of 11 Linux tasks. The public dev run wrote zero bytes to stdout. The final image was about 10.6 MB, and a local rebuild with the base image and dependencies cached took about 8.5 s.

## Pipeline

- Workers align input chunks to newline boundaries in parallel and scan delimiters with AVX2 where supported. Union-find builds the identity lookup table while the event scan runs. Click parsing starts as soon as that table is ready.
- Workers store 32-byte event candidates in per-worker slabs and partition by exact ID. A sparse exact hash table trades extra index space for shorter probes. The earliest copy wins, including when that copy fails validation.
- Counting and prefix sums group surviving rows by person. Workers reuse scatter buffers and schedule larger buckets first. They sort each person's history by timestamp, using radix sort for large event histories. A rolling click window tracks the crediting click and distinct assisting campaigns.
- Each worker streams user-day rows into Parquet. Campaign-day totals are merged across the workers.

Production dependencies provide Parquet serialization and read-only memory mapping. [parquet-go](https://github.com/parquet-go/parquet-go) · [golang.org/x/sys](https://pkg.go.dev/golang.org/x/sys)

The submission code is released under the MIT License.
