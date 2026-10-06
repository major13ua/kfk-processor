package xme.common.kfkprocessor.build;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gradle.testkit.runner.BuildResult;
import org.gradle.testkit.runner.GradleRunner;
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
        BuildResult r = run(Map.of(), "test", "printEnv", "-Premote").ok();
        assertThat(r.getOutput()).contains("TARGET-INPUT=remote", "UPTODATE=false");
    }

    @Test
    void localRunDiffersFromRemoteInTheModeInput() {
        BuildResult local = run(Map.of(), "test", "printEnv").ok();
        assertThat(local.getOutput()).contains("TARGET-INPUT=local").doesNotContain("TARGET-INPUT=remote");
    }
}
