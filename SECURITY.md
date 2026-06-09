# Security Policy

## Reporting Vulnerabilities

If you discover a security vulnerability in `sharded-sink`, report it privately.

**Do not open a public GitHub issue for security vulnerabilities.**

Report it via GitHub's private vulnerability reporting feature on the
[sharded-sink repository](https://github.com/godaddy/sharded-sink/security/advisories/new),
or contact the maintainers directly.

Include:

- A description of the vulnerability
- Steps to reproduce
- Potential impact
- A suggested fix (if you have one)

You will receive an acknowledgment within 72 hours. A fix will be developed and
released as quickly as possible, with credit given to the reporter (unless
anonymity is requested).

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

Only the latest release receives security fixes.

## Scope Notes

`sharded-sink` is a **lossy, fire-and-forget** in-process sink, not a durable
queue. By design it sheds items under overload and does not guarantee delivery,
ordering across shards, or persistence. It contains no `unsafe` code in the
library (`unsafe_code = "deny"`), performs no I/O of its own, and holds no
secrets — the data it carries is entirely caller-supplied. Treat the items you
push and whatever your `SinkAction` does with them as the security boundary.
