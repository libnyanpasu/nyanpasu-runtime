# Configuration check output fixtures

These stdout and stderr files are the audited Linux captures from 2026-11-05 for [libnyanpasu/clash-nyanpasu#5935](https://github.com/libnyanpasu/clash-nyanpasu/issues/5935), with only the private path prefix normalized to `/tmp/kernel-audit`. Timestamps, the remaining output, and empty stderr streams are preserved.

| Directory | Actual binary version |
| --- | --- |
| mihomo | Mihomo Meta v1.19.27 |
| mihomo-alpha | Mihomo Meta alpha-24b6de7 |
| clash | Clash Premium n2023-09-05-gdcc8d87 |
| clash-rs-official | clash-rs 0.10.10 |
| clash-rs-alpha-official | clash-rs 0.10.11-alpha+sha.cc960fc |
| meow | meow 0.22.0 |
| meow-alpha | meow 0.22.0-alpha+3c27aca |

The audit used an independent home for each check, disabled networking with a user/network namespace, and pre-seeded Premium's Country.mmdb so the check did not download it. The invocation was:

```sh
SAFE_PATHS="<isolated-home>:<config-parent>" CLICOLOR_FORCE=0 timeout 30 unshare --user --map-root-user --net <binary> -t -d <isolated-home> -f <absolute-config.yaml>
```

The audit inherited `NO_COLOR=1` and `TERM=xterm-256color`. It did not enable Meow strict mode or add extra Mihomo startup flags. Version probes used `-v` for Mihomo/Premium and `--version` for clash-rs/Meow.

The parser tests load only these tracked stream files. Paths inside them are capture data: tests obtain the invocation path from the captured trailer and never open the original path or require an installed kernel. Additional synthetic tests reproduce the Windows example retained in issue #5935, including the Chinese group name and a member containing a space.

`a_index_after_sort` reports Second at index 0 although the candidate places First before Second. `a_delimiters` uses group `G: 'H'` and member `X' not found: 'Y`, so the output has ambiguous field boundaries. Provider and proxy references share the group-member template. YAML, duplicate-name, and cycle cases exercise the fallback. The Rust kernels have no structured diagnostic support in this parser, and Meow can warn while its check succeeds.
