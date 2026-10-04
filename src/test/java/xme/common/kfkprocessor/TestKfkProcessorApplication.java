package xme.common.kfkprocessor;

import org.springframework.boot.SpringApplication;

public class TestKfkProcessorApplication {

    static void main(String[] args) {
        SpringApplication.from(KfkProcessorApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
