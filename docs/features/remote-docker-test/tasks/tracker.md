# Tracker: remote-docker-test

> Status of every task in the epic. `implement` updates `done` as it commits each task.
> States: `todo` · `in_progress` · `blocked` · `review` · `done`.

| # | Task | Layer | Owner | Estimate | Blocked by | Status |
|---|---|---|---|---|---|---|
| T1 | Check engine settings precedence and Ryuk socket handling, record the result | docs | I.Chupryna | S | — | done |
| T2 | Add the remote switch script plugin and wire the test task environment | wiring | I.Chupryna | M | T1 | done |
| T3 | Validate the remote address from the per-user setting and refuse bad forms | wiring | I.Chupryna | M | T2 | done |
| T4 | Open and close the ssh tunnel as a build service within the time budget | infra | I.Chupryna | M | T3 | done |
| T5 | Ping the remote engine through the tunnel and set the reaper socket override | infra | I.Chupryna | M | T4, T1 | done |
| T6 | Refuse loadTest and preReleaseTest when the remote switch is on | wiring | I.Chupryna | S | T2 | done |
| T7 | Test the build logic with Gradle TestKit using a fake ssh and fake engine | tests | I.Chupryna | M | T3, T5, T6 | done |
| T8 | Verify remote mode against the real remote host and record findings | tests | I.Chupryna | M | T7 | blocked |
| T9 | Write the developer how-to for remote mode | docs | I.Chupryna | S | T5 | done |

**Total:** 9 tasks, about 5 person-days.
