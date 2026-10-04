package xme.common.kfkprocessor.requestreply.adapters.allowance;

import java.util.Comparator;
import org.junit.jupiter.api.MethodDescriptor;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.MethodOrdererContext;

/** Runs container-stopping tests (name starts with "stopping") after all others; the shared Testcontainers instance cannot be restarted. */
public class OutageLastOrderer implements MethodOrderer {

    @Override
    public void orderMethods(MethodOrdererContext context) {
        context.getMethodDescriptors()
                .sort(Comparator.comparing((MethodDescriptor d) -> d.getMethod().getName().startsWith("stopping")));
    }
}
