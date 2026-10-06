# Spike findings — remote-docker-test (T1)

Source: Testcontainers configuration docs (https://java.testcontainers.org/features/configuration/), fetched 2026-10-06. Not run against a remote host; items marked UNVERIFIED are checked in T8.

## 1. Precedence (AC-01)

Documented order, highest first: environment variables, then `~/.testcontainers.properties`, then classpath `testcontainers.properties`.

- Env var set → wins over the user file. So in remote mode, setting `DOCKER_HOST`, `TESTCONTAINERS_HOST_OVERRIDE` and `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` for the test JVM is enough.
- Env var absent → the user file is used. Clearing variables does NOT force local: `docker.host`, `tc.host`, `host.override` in `~/.testcontainers.properties` still apply.
- Local machine check: `~/.testcontainers.properties` has only `docker.client.strategy=...UnixSocketClientProviderStrategy` (no host keys); no `DOCKER_*`/`TESTCONTAINERS_*` env vars; engine is OrbStack, local.

## 2. Outcome (decision)

Force local is NOT possible without knowing the local socket path. Chosen: **local mode clears the off-machine env variables, and additionally refuses with a message naming `~/.testcontainers.properties` if that file sets `docker.host`/`tc.host` to a non-unix address or sets `host.override`.** Spec AC-01 needs a note: "whatever container-host setting exists" holds for the environment (cleared) and for the user file (refused with a message, never silently remote).

## 3. Ryuk socket (UNVERIFIED)

Ryuk bind-mounts the engine socket path inside the engine's host. With `DOCKER_HOST` a forwarded local unix socket, the local path would be wrong on the remote host, so `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=<remoteDocker.socket>` (default `/var/run/docker.sock`) is set in remote mode. Confirm in T8 (real host); if Ryuk fails to start, record the working value there.

## 4. Open point

`docker.client.strategy` pinned in the user file to `UnixSocketClientProviderStrategy` (as on this machine): whether it honours a forwarded-socket `DOCKER_HOST` is UNVERIFIED, check in T8.
