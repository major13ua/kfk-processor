package xme.common.kfkprocessor.requestreply.autoconfig;

import java.util.ArrayList;
import java.util.List;

/** Minimal valid {@code xme.request-reply.*} properties shared by the T15 tests. */
final class RequestReplyTestProps {

    private RequestReplyTestProps() {
    }

    static String[] valid(String... extra) {
        List<String> p = new ArrayList<>(List.of(
                "xme.request-reply.worker-identity=worker-1",
                "xme.request-reply.reply-destination=replies",
                "xme.request-reply.rate-budget-per-second=1000",
                "xme.request-reply.lanes[0].name=high",
                "xme.request-reply.lanes[0].source=requests-high",
                "xme.request-reply.lanes[0].weight=3",
                "xme.request-reply.lanes[1].name=low",
                "xme.request-reply.lanes[1].source=requests-low",
                "xme.request-reply.lanes[1].weight=1"));
        p.addAll(List.of(extra));
        return p.toArray(String[]::new);
    }
}
