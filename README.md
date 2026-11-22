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

Both images ran with eight pinned vCPUs, a 24 GiB container limit, no swap, and read-only inputs from the `shopping-sessions-5` generator. Timings use Docker's container start and exit timestamps in a local Linux VM running amd64 through Rosetta on an Apple M1 Max. The 20-million-event warm result is the median of three interleaved runs per image. The cold result is the median of two pairs in reversed order. The harness requested eviction of the input from the guest page cache before each cold run. The 60-million-event measurements use two reversed-order pairs per storage mode. The RAM disk row uses the repeat after the initial trial. The VM owns the preloaded RAM disk pages.

| Seed | Events | Clicks | Input | Previous time (`670e824`) | Fused time | Fused cgroup peak |
| --- | ---: | ---: | --- | ---: | ---: | ---: |
| 22 | 20 million | 5 million | Warm | 1.627 s | 1.655 s | 1.21 GiB |
| 22 | 20 million | 5 million | Cold | 1.791 s | 1.759 s | 3.10 GiB |
| 42 | 60 million | 15 million | Warm, host-shared mount | 9.770 s | 9.857 s | 9.23 GiB |
| 42 | 60 million | 15 million | Warm, guest RAM disk | 6.326 s | 6.033 s | 3.29 GiB |

The fused build was faster in both cold practice pairs and both repeated RAM disk pairs. The warm practice and host-shared medians favored the baseline. Both Parquet tables matched the baseline row for row on both datasets.

## Pipeline

- Workers align input chunks to newline boundaries in parallel. The C++ event decoder combines AVX2 delimiter scanning, CSV validation, ID encoding, and timestamp decoding. It scans 64-byte spans and writes supported rows directly into the event-ID and person blocks. It decodes 13-digit epoch milliseconds with two eight-byte operations. Rows with variable-width IDs, purchases, wide timestamps, or exhausted native blocks return through the exact Go writer. Union-find builds the identity lookup table while events are scanned. Click parsing starts as soon as that table is ready. Long user IDs use a sharded string table. Click IDs reference their exact bytes in the mapped CSV through attribution.
- Workers store 16-byte event rows by person and partition event ID references into 512 deduplication buckets. Each activity uses one word for its timestamp, category and price, with exact overflow values stored separately. The deduplication table uses four-byte slots containing a row reference and fingerprint. A fingerprint match still checks the full key and, for long IDs, the exact bytes in the mapped CSV. The earliest copy wins, including when that copy fails validation. The native key buffers are released after deduplication. For rows parsed before identity links are ready, each worker caches recently resolved identities and moves surviving payloads whose canonical person falls in a different bucket.
- Counting and prefix sums group surviving rows by person. Workers reuse scatter buffers and schedule larger buckets first. Packed activities sort as integers, with radix sort for large histories and exact timestamp comparisons for histories containing overflow values. A single session can find its crediting click and distinct assisting campaigns in one pass. For multiple sessions, a rolling window uses clicks sorted by timestamp. Attribution compares click ID bytes only when qualifying timestamps tie.
- Each worker streams user-day rows into Parquet. Campaign-day totals are merged across the workers.

Production dependencies provide Parquet serialization and read-only memory mapping. [parquet-go](https://github.com/parquet-go/parquet-go) · [golang.org/x/sys](https://pkg.go.dev/golang.org/x/sys)

The submission code is released under the MIT License.
