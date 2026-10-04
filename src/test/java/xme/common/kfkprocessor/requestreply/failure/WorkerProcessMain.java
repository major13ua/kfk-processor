package xme.common.kfkprocessor.requestreply.failure;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.Banner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import xme.common.kfkprocessor.requestreply.api.RequestReplyHandler;

/**
 * A worker in its own JVM so the suite can kill it with SIGKILL. Arguments: {@code key=value} properties.
 * System properties: {@code t16.log} (file receiving "tag idempotencyKey correlationId" per Handler call),
 * {@code t16.tag}, {@code t16.mode} (block: Handler never returns; answer: returns "pong:" + request).
 */
public final class WorkerProcessMain {

    private WorkerProcessMain() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, Object> props = new LinkedHashMap<>();
        for (String a : args) {
            int i = a.indexOf('=');
            props.put(a.substring(0, i), a.substring(i + 1));
        }
        Path log = Path.of(System.getProperty("t16.log"));
        String tag = System.getProperty("t16.tag");
        boolean block = "block".equals(System.getProperty("t16.mode"));
        RequestReplyHandler<String, String, String> handler = (ctx, req) -> {
            Files.writeString(log, tag + " " + ctx.idempotencyKey() + " " + ctx.correlationId() + "\n",
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND,
                    StandardOpenOption.SYNC);
            if (block) {
                Thread.sleep(Long.MAX_VALUE);
            }
            return "pong:" + req;
        };
        ApplicationContextInitializer<ConfigurableApplicationContext> init = ctx -> {
            GenericApplicationContext g = (GenericApplicationContext) ctx;
            g.registerBean("handler", RequestReplyHandler.class, () -> handler);
        };
        new SpringApplicationBuilder(FailureHarness.Base.class).web(WebApplicationType.NONE)
                .bannerMode(Banner.Mode.OFF).logStartupInfo(false).properties(props).initializers(init).run();
        Thread.currentThread().join();
    }
}
