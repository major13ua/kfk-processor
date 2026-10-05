package xme.common.kfkprocessor.requestreply.failure;

import static org.assertj.core.api.Assertions.assertThat;
import static xme.common.kfkprocessor.requestreply.failure.FailureHarness.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Testcontainers;

/** AC-03 and AC-05: a worker killed (SIGKILL) mid-Cycle, or after a commit, and restarted with the same identity. */
@Testcontainers
class WorkerKilledMidCycleIT {

    @TempDir
    Path tmp;

    @Test
    void killedMidCycleThenRestartedSameIdentityGivesSameIdempotencyKeyAndOneCommittedReply() throws Exception {
        Ids ids = Ids.fresh();
        Path log = tmp.resolve("handler.log");
        Files.createFile(log);
        // long timeouts so that nothing but the kill ends the first Cycle
        Map<String, Object> props = workerProps(ids, 1000, 100, Duration.ofSeconds(60), Duration.ofSeconds(100));
        List<String> requested = List.of("kill-1");
        produce(ids.requests(), requested);

        Process first = launch(props, log, "first", "block");
        try {
            await("first worker's Handler started on the request", WAIT, () -> lines(log).size() >= 1);
            assertThat(readCommitted(ids.replies(), Duration.ofSeconds(1)))
                    .as("nothing committed before the kill").isEmpty();
            first.destroyForcibly();
            assertThat(first.waitFor(30, TimeUnit.SECONDS)).as("killed worker exited").isTrue();
        } finally {
            first.destroyForcibly();
        }

        Process second = launch(props, log, "second", "answer");
        try {
            List<Rep> replies = awaitReplies(ids.replies(), 1, WAIT, Duration.ofSeconds(3));

            List<String> lines = lines(log);
            assertThat(lines).as("Handler runs: first (killed) then second (restart)").hasSize(2);
            String keyFirst = lines.get(0).split(" ")[1];
            String keySecond = lines.get(1).split(" ")[1];
            assertThat(lines.get(0)).startsWith("first ");
            assertThat(lines.get(1)).startsWith("second ");
            assertThat(keySecond).as("Idempotency Key after restart").isEqualTo(keyFirst);

            assertReconciled(requested, replies);
            assertThat(replies).hasSize(1);
            assertThat(replies.get(0).body()).isEqualTo("pong:req-kill-1");
        } finally {
            second.destroyForcibly();
            second.waitFor(30, TimeUnit.SECONDS);
        }
    }

    // test-plan edge case: killed after commit but before the next fetch: no replay, no duplicate
    @Test
    void killedAfterCommitBeforeNextFetchIsNotReplayedAfterRestart() throws Exception {
        Ids ids = Ids.fresh();
        Path log = tmp.resolve("handler.log");
        Files.createFile(log);
        Map<String, Object> props = workerProps(ids, 1000, 100, Duration.ofSeconds(5), Duration.ofSeconds(20));
        List<String> committed = List.of("done-1", "done-2", "done-3");
        produce(ids.requests(), committed);

        Process first = launch(props, log, "first", "answer");
        try {
            awaitReplies(ids.replies(), committed.size(), WAIT, Duration.ZERO);
            // the Cycle is committed and the lane holds nothing newer: the next fetch has nothing to read
            first.destroyForcibly();
            assertThat(first.waitFor(30, TimeUnit.SECONDS)).as("killed worker exited").isTrue();
        } finally {
            first.destroyForcibly();
        }
        assertThat(lines(log)).as("first worker handled each request once").hasSize(committed.size());

        Process second = launch(props, log, "second", "answer");
        try {
            produce(ids.requests(), List.of("after-1")); // proves the restarted worker is fetching
            List<String> all = new ArrayList<>(committed);
            all.add("after-1");
            List<Rep> replies = awaitReplies(ids.replies(), all.size(), WAIT, Duration.ofSeconds(3));

            assertReconciled(all, replies);
            List<String> secondRuns = lines(log).stream().filter(l -> l.startsWith("second ")).toList();
            assertThat(secondRuns).as("restart replays nothing committed, handles only the new request")
                    .hasSize(1).allSatisfy(l -> assertThat(l).endsWith(" after-1"));
        } finally {
            second.destroyForcibly();
            second.waitFor(30, TimeUnit.SECONDS);
        }
    }

    private Process launch(Map<String, Object> props, Path log, String tag, String mode) throws IOException {
        List<String> cmd = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx256m",
                "-Dt16.log=" + log, "-Dt16.tag=" + tag, "-Dt16.mode=" + mode,
                "-cp", System.getProperty("java.class.path"), WorkerProcessMain.class.getName()));
        props.forEach((k, v) -> cmd.add(k + "=" + v));
        return new ProcessBuilder(cmd).redirectErrorStream(true)
                .redirectOutput(tmp.resolve(tag + ".out").toFile()).start();
    }

    private static List<String> lines(Path log) {
        try {
            return Files.readAllLines(log);
        } catch (IOException e) {
            return List.of();
        }
    }
}
