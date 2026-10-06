# Run the tests on your remote host

Default: tests start Kafka and Redis containers on your own machine. With `-Premote` they start on your own remote host instead. Nothing falls back to local: if the remote host does not work, the run stops.

## One-time setup (about 10 minutes)

1. **ssh access.** `ssh user@your-host` must work without a password prompt (key or agent). The `ssh` command must exist on your machine.
2. **Engine on the remote host.** A container engine (Docker) runs there and your user may use its socket (`/var/run/docker.sock` by default).
3. **Network.** Your machine must reach the container ports on the remote host (VPN on, firewall allows ephemeral ports 1024-65535 from your machine). The tunnel only carries the engine socket, not container ports.
4. **Your address, once per machine**, in `~/.gradle/gradle.properties` (never in the project):

   ```properties
   remoteDocker.host=ssh://user@your-host
   # optional, default /var/run/docker.sock (socket path on the remote host)
   remoteDocker.socket=/var/run/docker.sock
   ```

   Only `ssh://user@host` is accepted. `tcp://` and `http://` are refused.

## Use

```bash
./gradlew test -Premote     # containers on the remote host
./gradlew test              # containers on this machine (default, unchanged)
```

Every run prints its target first: `[remote-docker] Container target: local` or `... remote ssh://user@your-host`.

IDE: run tests through Gradle (not the IDE's own runner) and add `-Premote` to the Gradle run configuration's arguments. The IDE's own runner ignores the switch and runs locally.

`loadTest` and `preReleaseTest` are local-only and are refused with `-Premote`: network latency distorts their timings. The performance tests inside `test` do run remotely.

## When the run stops

| Message | Meaning |
|---|---|
| `remote mode needs the setting 'remoteDocker.host'` | step 4 missing |
| `must be a secure-shell address` | address is not `ssh://user@host` |
| `could not open the tunnel to ...` | host down, VPN off, key rejected or no `ssh` command, within 30 s |
| `container service on the remote host ... is not running` | ssh works but Docker is stopped there |
| `~/.testcontainers.properties sets docker.host...` | local run refused: that file would send containers to another machine; remove those keys or use `-Premote` |

## Limits

- Leftover containers on the remote host (after a killed run) are cleaned by Testcontainers' reaper when it can; otherwise prune by hand: `ssh user@host docker container prune`.
- A dropped VPN mid-run kills the tunnel and fails the run; rerun after reconnecting.
- Timing-sensitive tests may be slower over the network. Measured numbers: see `docs/features/remote-docker-test/verification.md` (after the first real run).
