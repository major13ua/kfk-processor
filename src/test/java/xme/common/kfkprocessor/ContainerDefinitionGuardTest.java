package xme.common.kfkprocessor;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/** Container images are defined in {@link TestcontainersConfiguration} only; tests build containers via its factories. */
class ContainerDefinitionGuardTest {

    private static final List<String> FORBIDDEN = List.of("new KafkaContainer(", "DockerImageName.parse(");

    @Test
    void noTestConstructsContainersOutsideTheConfiguration() throws IOException {
        Path root = Path.of("src/test/java");
        try (Stream<Path> files = Files.walk(root)) {
            List<String> offenders = files
                    .filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> !p.getFileName().toString().equals("TestcontainersConfiguration.java"))
                    .filter(p -> !p.getFileName().toString().equals("ContainerDefinitionGuardTest.java"))
                    .filter(p -> FORBIDDEN.stream().anyMatch(f -> read(p).contains(f)))
                    .map(Path::toString)
                    .toList();
            assertThat(offenders).isEmpty();
        }
    }

    private static String read(Path p) {
        try {
            return Files.readString(p);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
