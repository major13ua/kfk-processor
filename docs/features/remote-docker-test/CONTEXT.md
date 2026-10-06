---
status: Living
updated_at: "2026-10-06"
---

# Domain Context — remote-docker-test

## Glossary

- Developer — a person who runs this repository's automated tests from their own machine and owns their own remote machine. NOT a CI system.
- Local mode — the default way of running tests, where all test containers start on the Developer's own machine. NOT remote mode, and it needs no switch.
- Remote host — the Developer's own remote machine on the private network that runs test containers for them. NOT a cloud service, NOT shared between Developers, and NOT the machine the Developer types on.
- Remote mode — a test run selected by one build switch, where all test containers start on the remote host instead of the Developer's machine. NOT a fallback: it is never chosen automatically.

## Invariants

- A test run never switches between local mode and remote mode on its own.
- A Developer's remote host address never appears in shared project files.
- A remote run never reuses the result of an earlier local run, and the reverse.

## Out of scope

- Continuous integration setup · CI keeps its current way of running containers.
- Provisioning the remote host · each Developer sets up their own machine.
- Shared remote hosts · each Developer has their own remote machine.
