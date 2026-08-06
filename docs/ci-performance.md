# CI performance baseline

Measured on 2026-08-06 for `main` at `0accd474` and the ticket branch's base at
`18094ee0`.

## Hosted-CI baseline

There was no hosted CI to profile. This is not a missing-data inference: the
repository and GitHub APIs consistently report that no CI exists.

- No CI definition is present anywhere in reachable Git history (`git
  rev-list --all --objects` and a history search for common CI paths).
- `GET /repos/QuillDev/nakode/actions/workflows` returned `total_count: 0`.
- `gh run list --repo QuillDev/nakode --limit 100` returned an empty list.
- The `main` head has zero check runs, and each of PRs #39, #40, #41, and #42
  has an empty check rollup.
- `main` has no branch protection and the repository has no webhooks.

Consequently, across several recent default-branch commits and PRs the measured
queue time, execution time, job/step time, retries, cache behavior, artifact
transfer, and critical path are all **not applicable**, rather than long. There
were no runners, matrices, concurrency rules, retries, timeouts, cancellations,
caches, or artifacts to inspect. A before/after hosted-run comparison cannot
honestly be claimed from this repository state.

This contradicts the report that a long-running Nakode CI check currently
exists. The likely long duration was an unrecorded local/external quality-gate
run or a check associated with a different repository. The GitHub evidence
above is the reproducible boundary.

## Local clean/warm baseline

The full documented quality gate was measured with isolated cargo and target
directories on an Apple M4 Max runner (`aarch64-apple-darwin`, 14 logical CPUs,
36 GiB RAM), Rust 1.97.1. Cargo used the locked dependency graph.

| Operation | Cache state | Wall time | Detail |
| --- | --- | ---: | --- |
| `cargo fetch --locked` | empty `CARGO_HOME` | 8.71 s | 1.1 GiB Cargo home after fetching all cross-platform lockfile packages |
| `cargo test --locked --all-targets --all-features --no-run --timings` | fetched deps, empty target | 70.34 s | 481 dirty units, max concurrency 16; 4.1 GiB target |
| `cargo test --locked --all-targets --all-features` | compiled target | 12.89 s | compile freshness 0.71 s; 517 tests, 0 ignored; longest integration binaries were `service_cli` 3.45 s and `tui_terminal` 3.27 s |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | after test build | 37.04 s | Clippy checks a distinct artifact graph even after test compilation |
| `cargo fmt --all -- --check` | warm | 0.81 s | no compilation |
| repeated full test | warm | 8.85 s | no rebuild; process and terminal integration tests dominate |
| repeated Clippy | warm | 0.68 s | no rebuild |

The clean local quality gate's serial critical path is approximately 120 s
(fetch 9 + test build/execution 83 + Clippy 37 + formatting 1). Test execution
itself is not pathological: all 517 tests pass, none are ignored, and the entire
warm test command takes under 13 s. The source contains bounded startup/network
fixtures, but no observed hang, retry storm, rate limit, or external-service
wait.

## Root-cause ranking

1. **No CI implementation or observability.** There are no real runs, step
   timings, required checks, timeouts, cancellation, or caches. This is the
   primary correctness problem and explains why the reported hosted duration
   cannot be reproduced or attributed.
2. **Clean compilation dominates genuine quality-gate time.** Compilation is
   70 s on a 14-core local machine, versus less than 13 s for execution. The
   dependency graph includes image, terminal, gRPC, HTTP/TLS, Discord, and
   bundled SQLite stacks. Network fetching is only 9 s locally.
3. **Serial test and Clippy commands duplicate artifact traversal.** Clippy
   takes another 37 s after the test build. They have the same failure
   semantics whether placed in parallel required jobs, so serializing them is
   unnecessary for feedback time.
4. **Unscoped build-output caching would be expensive.** A combined local
   target reached 4.1 GiB, including 940 MiB of incremental state. Blindly
   caching it would create substantial transfer and save time. CI therefore
   disables incremental output and uses `rust-cache`'s pruning and separate
   test/lint cache scopes rather than caching raw `target/`.
5. **Tests are not the bottleneck.** The only multi-second integration binaries
   perform meaningful service/terminal lifecycle checks. Sharding or dropping
   them would add setup and compilation duplication for little wall-time gain.

## Implemented workflow

`.github/workflows/required.yml` establishes two plainly named, required-quality
jobs on pull requests and `main`:

- `Lint (fmt + clippy)` preserves formatting and pedantic Clippy with warnings
  denied.
- `Test (all targets + features)` preserves the repository's complete test
  command, including all features and integration targets.

The jobs run concurrently, so the intended cold critical path is the slower
full test build rather than tests plus Clippy. Each has a separately scoped,
lockfile/toolchain/runner-aware Rust cache; first-time contributors and forks
remain correct on a miss. Incremental state is disabled because a hosted runner
cannot reuse workspace incremental artifacts reliably and the measured state
alone was 940 MiB. Every action is commit-pinned, permissions are read-only, no
secrets are exposed, and the workflow neither uploads nor downloads build
artifacts.

Superseded commits cancel only within the same PR number (or the `main` ref), so
unrelated PRs cannot cancel each other. The test and lint jobs have explicit 30
and 20 minute bounds. Cache hit status and final target size are retained in the
step summary/logs without flooding normal output.

## Real-run verification procedure

After pushing this branch, inspect the cold run and then push an empty commit or
a documentation-only commit for the warm run:

```sh
gh run list --repo QuillDev/nakode --branch perf/reduce-nakode-ci-times \
  --workflow Required --limit 5
gh run view <run-id> --repo QuillDev/nakode --json \
  createdAt,startedAt,updatedAt,jobs,url
```

Record queue delay, both job and every step duration, cache-hit output, target
size, end-to-end wall time, and the final critical path here. Because there was
no prior CI run, compare cold versus warm revised runs and the measured local
serial gate; do not label that as a historical hosted before/after result.
