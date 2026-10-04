package xme.common.kfkprocessor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest(properties = "xme.request-reply.enabled=false")
class KfkProcessorApplicationTests {

    @Test
    void contextLoads() {
    }

}
