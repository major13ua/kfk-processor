package xme.common.kfkprocessor.build;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Build-logic tests for gradle/remote-docker.gradle: a throwaway project applies the real script and a fake
 * {@code ssh} command stands in for the remote host. No container is started.
 */
class RemoteDockerBuildTest {

    @TempDir
    Path project;

    @TempDir
    Path home;

    @BeforeEach
    void setUp() throws IOException {
        Files.writeString(project.resolve("settings.gradle"), "rootProject.name = 'fake'\n");
        Files.createDirectories(project.resolve("gradle"));
        Files.copy(Path.of("gradle/remote-docker.gradle"), project.resolve("gradle/remote-docker.gradle"));
        Files.writeString(project.resolve("build.gradle"), """
                plugins { id 'java' }
                tasks.register('loadTest', Test) { }
                tasks.register('preReleaseTest', Test) { }
                apply from: 'gradle/remote-docker.gradle'
                tasks.register('printEnv') {
                    doLast {
                        def t = tasks.named('test').get()
                        ['DOCKER_HOST', 'TESTCONTAINERS_HOST_OVERRIDE', 'TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE']
                            .each { println 'ENV ' + it + '=' + t.environment.get(it) }
                        println 'TARGET-INPUT=' + t.inputs.properties.get('containerTarget')
                        println 'UPTODATE=' + t.outputs.upToDateSpec.isSatisfiedBy(t)
                    }
                }
                """);
    }

    Run run(Map<String, String> env, String... args) {
        Map<String, String> full = new HashMap<>(Map.of("PATH", System.getenv("PATH"), "HOME", home.toString()));
        full.putAll(env);
        List<String> all = new ArrayList<>(List.of(args));
        all.add("-s");
        GradleRunner runner = GradleRunner.create().withProjectDir(project.toFile()).withEnvironment(full)
                .withArguments(all);
        return new Run(runner);
    }

    record Run(GradleRunner runner) {
        BuildResult ok() {
            return runner.build();
        }

        BuildResult fail() {
            return runner.buildAndFail();
        }
    }

    // AC-07, AC-01
    @Test
    void noSwitchPrintsLocalTarget() {
        BuildResult r = run(Map.of(), "test", "printEnv").ok();
        assertThat(r.getOutput()).contains("[remote-docker] Container target: local");
        assertThat(r.getOutput()).contains("TARGET-INPUT=local");
    }

    // AC-01: off-machine settings in the environment are cleared, a local socket is left alone
    @Test
    void localModeClearsOffMachineEnvironment() {
        BuildResult r = run(Map.of("DOCKER_HOST", "tcp://10.0.0.5:2375", "TESTCONTAINERS_HOST_OVERRIDE", "10.0.0.5",
                "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/x.sock"), "test", "printEnv").ok();
        assertThat(r.getOutput()).contains("ENV DOCKER_HOST=null", "ENV TESTCONTAINERS_HOST_OVERRIDE=null",
                "ENV TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=null");
    }

    @Test
    void localModeKeepsLocalSocket() {
        BuildResult r = run(Map.of("DOCKER_HOST", "unix:///Users/me/.colima/docker.sock"), "test", "printEnv").ok();
        assertThat(r.getOutput()).contains("ENV DOCKER_HOST=unix:///Users/me/.colima/docker.sock");
    }

    // AC-03, AC-03b: the mode is a task input; a remote run is never up to date
    @Test
    void remoteRunIsNeverUpToDateAndModeIsAnInput() {
        engine = Engine.answering(sock);
        BuildResult r = remote("test", "printEnv").ok();
        assertThat(r.getOutput()).contains("TARGET-INPUT=remote", "UPTODATE=false");
    }

    @Test
    void localRunDiffersFromRemoteInTheModeInput() {
        BuildResult local = run(Map.of(), "test", "printEnv").ok();
        assertThat(local.getOutput()).contains("TARGET-INPUT=local").doesNotContain("TARGET-INPUT=remote");
    }

    // ---- remote mode: fake ssh + fake engine ----

    static final String ADDRESS = "ssh://dev@box.example";

    Path sock;
    Path fakeSsh;
    Engine engine;

    @AfterEach
    void stopEngine() {
        if (engine != null) engine.close();
    }

    void fakeSsh(String body) throws IOException {
        fakeSsh = home.resolve("fake-ssh.sh");
        Files.writeString(fakeSsh, "#!/bin/bash\necho \"$@\" >> " + home.resolve("ssh-args") + "\n" + body + "\n");
        fakeSsh.toFile().setExecutable(true);
    }

    Run remote(String... tasks) {
        return remoteWith(ADDRESS, tasks);
    }

    Run remoteWith(String address, String... tasks) {
        List<String> args = new ArrayList<>(List.of(tasks));
        args.addAll(List.of("-Premote", "-PremoteDocker.sshCommand=" + fakeSsh, "-PremoteDocker.localSocket=" + sock,
                "-PremoteDocker.timeoutSeconds=6", "-PremoteDocker.userFile=" + home.resolve("none")));
        if (address != null) args.add("-PremoteDocker.host=" + address);
        return run(Map.of(), args.toArray(String[]::new));
    }

    @BeforeEach
    void remoteSetUp() throws IOException {
        sock = Path.of("/tmp", "rd-" + Long.toHexString(System.nanoTime()) + ".sock");
        fakeSsh("sleep 60");
    }

    // AC-02 (build side): target banner names the address, engine env points at the tunnel and the remote host
    @Test
    void remoteRunStatesTargetAndSetsEngineEnvironment() {
        engine = Engine.answering(sock);
        BuildResult r = remote("test", "printEnv").ok();
        assertThat(r.getOutput()).contains("[remote-docker] Container target: remote " + ADDRESS,
                "ENV DOCKER_HOST=unix://" + sock, "ENV TESTCONTAINERS_HOST_OVERRIDE=box.example",
                "ENV TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock");
    }

    // AC-02: ssh forwards the remote engine socket; custom remote socket is used for the forward and the reaper
    @Test
    void customRemoteSocketIsForwardedAndPassedToTheReaper() throws IOException {
        // like real ssh: record the arguments, then create the forwarded local socket and answer pings on it
        fakeSsh("""
                python3 - "$@" <<'PY'
                import socket, sys, os
                spec = sys.argv[sys.argv.index('-L') + 1]
                path = spec.split(':')[0]
                s = socket.socket(socket.AF_UNIX); s.bind(path); s.listen(5)
                while True:
                    c, _ = s.accept(); c.recv(512); c.sendall(b'HTTP/1.0 200 OK\\r\\n\\r\\nOK'); c.close()
                PY
                """);
        BuildResult r = run(Map.of(), "test", "printEnv", "-Premote", "-PremoteDocker.sshCommand=" + fakeSsh,
                "-PremoteDocker.localSocket=" + sock, "-PremoteDocker.host=" + ADDRESS,
                "-PremoteDocker.socket=/run/user/1000/docker.sock", "-PremoteDocker.userFile=" + home.resolve("none"))
                .ok();
        assertThat(r.getOutput()).contains("ENV TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/run/user/1000/docker.sock");
        assertThat(Files.readString(home.resolve("ssh-args")))
                .contains("-N", "BatchMode=yes", "-L " + sock + ":/run/user/1000/docker.sock", ADDRESS);
    }

    // AC-04: missing setting stops before any tunnel, naming the setting and the file
    @Test
    void missingAddressStopsAndNamesSettingAndFile() {
        BuildResult r = remoteWith(null, "test").fail();
        assertThat(r.getOutput()).contains("remoteDocker.host", "~/.gradle/gradle.properties")
                .doesNotContain("Container target");
        assertThat(home.resolve("ssh-args")).doesNotExist();
    }

    // AC-10: anything that is not an ssh address is refused
    @Test
    void nonSshAddressesAreRefused() {
        for (String bad : List.of("tcp://10.0.0.5:2375", "http://box.example", "ssh://", "box")) {
            BuildResult r = remoteWith(bad, "test").fail();
            assertThat(r.getOutput()).as(bad).contains("secure-shell address");
        }
        assertThat(home.resolve("ssh-args")).doesNotExist();
    }

    // AC-10: an ssh address without a user gets its own message
    @Test
    void sshAddressWithoutUserIsRefusedWithItsOwnMessage() {
        BuildResult r = remoteWith("ssh://box.example", "test").fail();
        assertThat(r.getOutput()).contains("user is required").contains("ssh://user@host");
        assertThat(home.resolve("ssh-args")).doesNotExist();
    }

    // AC-05: no address in the shared project files
    @Test
    void projectFilesHoldNoRemoteHostAddress() throws IOException {
        for (String f : List.of("build.gradle", "gradle/remote-docker.gradle")) {
            assertThat(Files.readString(Path.of(f))).as(f).doesNotContainPattern("remoteDocker\\.host\\s*=\\s*ssh://(?!user@host)");
        }
        assertThat(Path.of("gradle.properties")).doesNotExist();
    }

    // AC-06: tunnel never opens (host down): stops within the budget, names the address
    @Test
    void unreachableHostStopsWithinBudgetNamingAddress() throws IOException {
        fakeSsh("echo 'ssh: connect to host box.example port 22: Operation timed out' >&2; exit 255");
        long t0 = System.nanoTime();
        BuildResult r = remote("test").fail();
        assertThat(r.getOutput()).contains(ADDRESS, "could not open the tunnel").doesNotContain("Container target");
        assertThat((System.nanoTime() - t0) / 1_000_000_000L).isLessThan(30);
    }

    @Test
    void hangingTunnelStopsWhenBudgetIsSpent() throws IOException {
        // ssh stays up but never creates the socket
        BuildResult r = remote("test").fail();
        assertThat(r.getOutput()).contains(ADDRESS, "did not open the tunnel in time");
    }

    @Test
    void missingSshCommandIsNamed() {
        BuildResult r = run(Map.of(), "test", "-Premote", "-PremoteDocker.sshCommand=/no/such/ssh",
                "-PremoteDocker.host=" + ADDRESS, "-PremoteDocker.userFile=" + home.resolve("none")).fail();
        assertThat(r.getOutput()).contains("/no/such/ssh", "could not be started");
    }

    // AC-08: ssh ok but the container service is not running
    @Test
    void stoppedContainerServiceIsReported() {
        engine = Engine.closingImmediately(sock);
        BuildResult r = remote("test").fail();
        assertThat(r.getOutput()).contains("container service on the remote host " + ADDRESS, "not running")
                .doesNotContain("Container target");
    }

    // AC-09: load commands are refused in remote mode
    @Test
    void loadCommandsAreRefusedInRemoteMode() {
        for (String cmd : List.of("loadTest", "preReleaseTest")) {
            BuildResult r = remote(cmd).fail();
            assertThat(r.getOutput()).as(cmd).contains("local-only");
        }
        BuildResult both = remote("test", "preReleaseTest").fail();
        assertThat(both.getOutput()).contains("local-only");
        assertThat(home.resolve("ssh-args")).doesNotExist();
    }

    // AC-01: a user file that points off the machine is refused in local mode with the file named
    @Test
    void localModeRefusesOffMachineUserFile() throws IOException {
        Path file = home.resolve("tc.properties");
        Files.writeString(file, "docker.host=tcp://10.0.0.5:2375\n");
        BuildResult r = run(Map.of(), "test", "-PremoteDocker.userFile=" + file).fail();
        assertThat(r.getOutput()).contains(file.toString(), "docker.host");
        Files.writeString(file, "docker.client.strategy=org.testcontainers.dockerclient.UnixSocketClientProviderStrategy\n");
        assertThat(run(Map.of(), "test", "-PremoteDocker.userFile=" + file).ok().getOutput())
                .contains("Container target: local");
    }

    /** A fake container engine on a unix socket. */
    static final class Engine implements AutoCloseable {
        private final ServerSocketChannel server;
        private final Thread thread;

        private Engine(Path sock, boolean answer) throws IOException {
            server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
            server.bind(UnixDomainSocketAddress.of(sock));
            thread = new Thread(() -> {
                try {
                    while (true) {
                        try (SocketChannel c = server.accept()) {
                            if (answer) {
                                c.read(ByteBuffer.allocate(512));
                                c.write(ByteBuffer.wrap("HTTP/1.0 200 OK\r\n\r\nOK".getBytes(StandardCharsets.US_ASCII)));
                            }
                        }
                    }
                } catch (IOException ignored) {
                    // closed
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        static Engine answering(Path sock) {
            return create(sock, true);
        }

        static Engine closingImmediately(Path sock) {
            return create(sock, false);
        }

        private static Engine create(Path sock, boolean answer) {
            try {
                return new Engine(sock, answer);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void close() {
            try {
                server.close();
            } catch (IOException ignored) {
                // closing
            }
        }
    }
}
