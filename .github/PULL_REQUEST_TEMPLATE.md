## What & why

<!-- One or two sentences: what changes and why it's needed. -->

## Checklist

- [ ] CI passes (lint / tests)
- [ ] No secrets, tokens, or credential files in the diff
- [ ] Touches auth, credentials, external APIs, or file/network I/O → security-reviewer agent was run
- [ ] dbt models changed → `dbt build` passes against the committed fixture
