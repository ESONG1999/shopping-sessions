# Shopping Sessions

Go submission for the Databricks Shopping Sessions track with a C++/AVX2 event decoder. The binary reads the mounted clickstream data and writes `user_daily/` and `campaign_daily/` as Parquet. [Challenge rules](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/rules.md?inline=1)

## Run

```sh
make run-container DATA=/path/to/data OUT=/path/to/output
```

The Makefile targets `linux/amd64` and packages a statically linked binary in a scratch image. The image runs as UID 1000, reads `DATA_DIR` (default `/data`), and writes to `OUTPUT_DIR` (default `/output`). It needs no network or scratch files at runtime. The native Makefile build uses the Go decoder. The container selects it when the CPU lacks the instructions required by the C++ decoder.

## Verify

Download and unpack the official DuckDB starter, generator, and dev answer. Use the generator and answer published for `shopping-sessions-5` to verify the variable-width IDs. [Track downloads](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions)

Build the generator with `cargo build --release --locked` in the generator folder and add its `target/release` directory to `PATH`. Install `polars` in the Python environment used by the starter's checkers. Run the checks on macOS or Linux with Go 1.22 or newer.

```sh
make check-fixtures STARTER=/path/to/starter-duckdb
make dev
make run-dev
make check-dev STARTER=/path/to/starter-duckdb ANSWER=/path/to/dev-answer
go test ./...
```

The official checkers passed all 36 fixtures and matched both Parquet schemas and all rows on the one-million-event dev set. In the `linux/amd64` image, both decoders matched the dev answer with offline, read-only execution as UID 1000.

## Local benchmark

Both images ran with eight pinned vCPUs, a 24 GiB container limit, no swap, and read-only inputs from the `shopping-sessions-5` generator. Wall time includes startup in a local Linux VM running amd64 through Rosetta on an Apple M1 Max. The 20-million-event warm result is the median of three interleaved runs per image. The cold result is the median of two pairs with reversed image order. The harness requested eviction of the input files from the guest page cache before each cold run. The 60-million-event result is a single pair using the same host-shared mount for both images.

| Seed | Events | Clicks | Cache | Previous wall time (`ee25e74`) | Optimized wall time | Optimized cgroup peak |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| 22 | 20 million | 5 million | Warm | 11.97 s | 2.34 s | 1.71 GiB |
| 22 | 20 million | 5 million | Cold | 12.92 s | 2.39 s | 3.58 GiB |
| 42 | 60 million | 15 million | Warm, host-shared mount | 52.19 s | 18.00 s | 10.61 GiB |

Both Parquet tables matched the previous version row for row on both datasets. The optimized image was faster in all three practice warm pairs and both cold pairs. On the cold runs the practice cgroup peak included about 1.91 GiB of file cache, with zero memory-limit events.

## Pipeline

- Workers align input chunks to newline boundaries in parallel. The C++ event decoder combines AVX2 delimiter scanning, CSV validation, ID encoding, and timestamp decoding in batches. It returns packed IDs or offsets into the original input for variable-width IDs. Union-find builds the identity lookup table while events are scanned. Click parsing starts as soon as that table is ready. The string table for long user and click IDs uses independently locked shards.
- Workers store event payloads by person and partition 16-byte references by event ID. Deduplication compares long event IDs byte for byte when their hashes match, using the mapped CSV until the comparison finishes. The earliest copy wins, including when that copy fails validation. For rows parsed before identity links are ready, only surviving payloads whose canonical person falls in a different bucket need to move.
- Counting and prefix sums group surviving rows by person. Workers reuse scatter buffers and schedule larger buckets first. They sort each person's history by timestamp, using radix sort for large event histories. A rolling click window tracks the crediting click and distinct assisting campaigns.
- Each worker streams user-day rows into Parquet. Campaign-day totals are merged across the workers.

Production dependencies provide Parquet serialization and read-only memory mapping. [parquet-go](https://github.com/parquet-go/parquet-go) · [golang.org/x/sys](https://pkg.go.dev/golang.org/x/sys)

The submission code is released under the MIT License.
