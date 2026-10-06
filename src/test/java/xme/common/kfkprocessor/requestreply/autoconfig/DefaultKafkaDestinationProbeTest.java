package xme.common.kfkprocessor.requestreply.autoconfig;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.TransactionalIdNotFoundException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import xme.common.kfkprocessor.requestreply.ports.ReplyDestinationFault;

/** B10 / AC-09: what the default probe checks and how it maps the broker's answers. No broker, Admin is a mock. */
class DefaultKafkaDestinationProbeTest {

    private static final String REPLIES = "replies";
    private static final List<String> LANES = List.of("lane-high", "lane-low");
    private static final String TXN = "request-reply-3:grp:worker-1";

    private final Admin admin = mock(Admin.class);
    private final List<Map<String, Object>> created = new ArrayList<>();

    private DefaultKafkaDestinationProbe probe() {
        return new DefaultKafkaDestinationProbe(Map.of("bootstrap.servers", "b:9092", "security.protocol", "SASL_SSL"),
                REPLIES, LANES, TXN, props -> {
                    created.add(props);
                    return admin;
                });
    }

    private static TopicDescription topic(String name, AclOperation... ops) {
        return new TopicDescription(name, false, List.of(), Set.of(ops));
    }

    private void topics(TopicDescription... descriptions) {
        Map<String, TopicDescription> m = new java.util.LinkedHashMap<>();
        for (TopicDescription d : descriptions) {
            m.put(d.name(), d);
        }
        describeTopicsReturns(KafkaFuture.completedFuture(m));
    }

    private void describeTopicsReturns(KafkaFuture<Map<String, TopicDescription>> all) {
        DescribeTopicsResult r = mock(DescribeTopicsResult.class);
        when(r.allTopicNames()).thenReturn(all);
        when(admin.describeTopics(any(java.util.Collection.class), any(DescribeTopicsOptions.class))).thenReturn(r);
    }

    private void transaction(KafkaFuture<TransactionDescription> outcome) {
        DescribeTransactionsResult r = mock(DescribeTransactionsResult.class);
        when(r.description(TXN)).thenReturn(outcome);
        when(admin.describeTransactions(any(java.util.Collection.class))).thenReturn(r);
    }

    private static KafkaFuture<TransactionDescription> failed(Throwable t) {
        KafkaFutureImpl<TransactionDescription> f = new KafkaFutureImpl<>();
        f.completeExceptionally(t);
        return f;
    }

    /** A real result object (values() and all() both work), so the test does not pin which accessor the probe uses. */
    private void cleanupPolicy(KafkaFuture<Config> outcome) {
        Map<ConfigResource, KafkaFuture<Config>> m = Map.of(new ConfigResource(ConfigResource.Type.TOPIC, REPLIES),
                outcome);
        DescribeConfigsResult r = new DescribeConfigsResult(m) { };
        when(admin.describeConfigs(any(java.util.Collection.class))).thenReturn(r);
        when(admin.describeConfigs(any(java.util.Collection.class),
                any(org.apache.kafka.clients.admin.DescribeConfigsOptions.class))).thenReturn(r);
    }

    private void cleanupPolicy(String policy) {
        cleanupPolicy(KafkaFuture.completedFuture(new Config(List.of(new ConfigEntry("cleanup.policy", policy)))));
    }

    private void allGranted() {
        cleanupPolicy("delete");
        topics(topic(REPLIES, AclOperation.WRITE, AclOperation.DESCRIBE),
                topic("lane-high", AclOperation.READ, AclOperation.DESCRIBE),
                topic("lane-low", AclOperation.READ, AclOperation.DESCRIBE));
        transaction(failed(new TransactionalIdNotFoundException("none yet")));
    }

    @Test
    void passesWhenReplyTopicIsWritableLanesReadableAndTransactionalIdDescribable() {
        allGranted();
        assertDoesNotThrow(() -> probe().probe());
    }

    @Test
    void replyTopicWithoutWriteIsPermissionDenied() {
        allGranted();
        topics(topic(REPLIES, AclOperation.DESCRIBE),
                topic("lane-high", AclOperation.READ), topic("lane-low", AclOperation.READ));
        assertInstanceOf(ReplyDestinationFault.PermissionDenied.class,
                assertThrows(ReplyDestinationFault.class, () -> probe().probe()));
    }

    @Test
    void laneWithoutReadIsPermissionDeniedNamingTheLane() {
        allGranted();
        topics(topic(REPLIES, AclOperation.WRITE),
                topic("lane-high", AclOperation.READ), topic("lane-low", AclOperation.DESCRIBE));
        ReplyDestinationFault e = assertThrows(ReplyDestinationFault.class, () -> probe().probe());
        assertInstanceOf(ReplyDestinationFault.PermissionDenied.class, e);
        assertTrue(e.getMessage().contains("lane-low"), e.getMessage());
    }

    @Test
    void transactionalIdAuthorizationFailureIsPermissionDeniedNotUnavailable() {
        allGranted();
        transaction(failed(new TransactionalIdAuthorizationException("no describe on the transactional id")));
        ReplyDestinationFault e = assertThrows(ReplyDestinationFault.class, () -> probe().probe());
        assertInstanceOf(ReplyDestinationFault.PermissionDenied.class, e);
        assertTrue(e.getMessage().toLowerCase().contains("transactional"), e.getMessage());
    }

    @Test
    void anUnknownTransactionalIdIsFine() {
        allGranted();
        transaction(failed(new TransactionalIdNotFoundException("none yet")));
        assertDoesNotThrow(() -> probe().probe());
    }

    @Test
    void aBrokerFailureIsUnavailable() {
        allGranted();
        KafkaFutureImpl<Map<String, TopicDescription>> down = new KafkaFutureImpl<>();
        down.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("broker down"));
        describeTopicsReturns(down);
        assertInstanceOf(ReplyDestinationFault.Unavailable.class,
                assertThrows(ReplyDestinationFault.class, () -> probe().probe()));
    }

    @Test
    void theAdminClientGetsTheSecurityPropertiesOfTheWorker() {
        allGranted();
        probe().probe();
        assertEquals(1, created.size());
        assertEquals("SASL_SSL", created.get(0).get("security.protocol"));
        assertEquals("b:9092", created.get(0).get("bootstrap.servers"));
    }

    // review r2 D: a probe against an unreachable cluster must not block on Admin close; it closes with zero wait
    @Test
    void adminIsClosedWithoutWaitingAfterEveryProbe() {
        allGranted();
        probe().probe();
        transaction(failed(new TransactionalIdAuthorizationException("denied")));
        assertThrows(ReplyDestinationFault.class, () -> probe().probe());

        org.mockito.Mockito.verify(admin, org.mockito.Mockito.times(2)).close(java.time.Duration.ZERO);
        org.mockito.Mockito.verify(admin, org.mockito.Mockito.never()).close();
    }

    // review r3 B4 / AC-09: TLS/SASL authentication failure is permission-denied, as in the sink and the lanes
    @Test
    void authenticationFailureIsPermissionDeniedNotUnavailable() {
        allGranted();
        KafkaFutureImpl<Map<String, TopicDescription>> f = new KafkaFutureImpl<>();
        f.completeExceptionally(new org.apache.kafka.common.errors.SaslAuthenticationException("bad credentials"));
        describeTopicsReturns(f);
        assertInstanceOf(ReplyDestinationFault.PermissionDenied.class,
                assertThrows(ReplyDestinationFault.class, () -> probe().probe()));
    }

    @Test
    void sslHandshakeFailureIsPermissionDeniedNotUnavailable() {
        allGranted();
        KafkaFutureImpl<Map<String, TopicDescription>> f = new KafkaFutureImpl<>();
        f.completeExceptionally(new org.apache.kafka.common.errors.SslAuthenticationException("handshake failed"));
        describeTopicsReturns(f);
        assertInstanceOf(ReplyDestinationFault.PermissionDenied.class,
                assertThrows(ReplyDestinationFault.class, () -> probe().probe()));
    }

    // review r3 B1: an invalid topic name must not come back as Unavailable (the loop would start and lose requests)
    @Test
    void invalidTopicIsNotReportedAsUnavailable() {
        allGranted();
        KafkaFutureImpl<Map<String, TopicDescription>> f = new KafkaFutureImpl<>();
        f.completeExceptionally(new org.apache.kafka.common.errors.InvalidTopicException("bad topic name"));
        describeTopicsReturns(f);
        RuntimeException e = assertThrows(RuntimeException.class, () -> probe().probe());
        assertInstanceOf(ReplyDestinationFault.Invalid.class, e);
        assertTrue(e.getCause() instanceof org.apache.kafka.common.errors.InvalidTopicException, "cause: " + e.getCause());
    }

    // F27: the Invalid fault names a config code and the topic so the operator can fix it
    @Test
    void invalidFaultNamesConfigCodeAndTopic() {
        allGranted();
        KafkaFutureImpl<Map<String, TopicDescription>> f = new KafkaFutureImpl<>();
        f.completeExceptionally(new org.apache.kafka.common.errors.InvalidTopicException("bad topic name"));
        describeTopicsReturns(f);
        RuntimeException e = assertThrows(ReplyDestinationFault.Invalid.class, () -> probe().probe());
        assertTrue(e.getMessage().contains("request_reply.config."), e.getMessage());
        assertTrue(e.getMessage().contains(REPLIES), e.getMessage());
    }

    // F33 / review r5 R5: reply records are keyless, a compacted topic rejects every one of them
    @Test
    void compactedReplyTopicIsInvalidNamingConfigCodeAndTopic() {
        allGranted();
        cleanupPolicy("compact");
        ReplyDestinationFault e = assertThrows(ReplyDestinationFault.class, () -> probe().probe());
        assertInstanceOf(ReplyDestinationFault.Invalid.class, e);
        assertTrue(e.getMessage().contains("request_reply.config.reply_destination_compacted"), e.getMessage());
        assertTrue(e.getMessage().contains(REPLIES), e.getMessage());
    }

    @Test
    void compactAndDeleteReplyTopicIsAlsoInvalid() {
        allGranted();
        cleanupPolicy("compact,delete");
        ReplyDestinationFault e = assertThrows(ReplyDestinationFault.class, () -> probe().probe());
        assertInstanceOf(ReplyDestinationFault.Invalid.class, e);
        assertTrue(e.getMessage().contains("request_reply.config.reply_destination_compacted"), e.getMessage());
    }

    @Test
    void deleteCleanupPolicyIsNotRefusedAndConfigWasDescribed() {
        allGranted();
        cleanupPolicy("delete");
        assertDoesNotThrow(() -> probe().probe());
        assertConfigDescribed();
    }

    // a config describe that fails (e.g. no DESCRIBE_CONFIGS ACL) is never reported as "compacted"
    @Test
    void failedConfigDescribeIsNotMisreportedAsCompacted() {
        allGranted();
        KafkaFutureImpl<Config> f = new KafkaFutureImpl<>();
        f.completeExceptionally(new org.apache.kafka.common.errors.TopicAuthorizationException("no describe configs"));
        cleanupPolicy(f);
        assertDoesNotThrow(() -> probe().probe());
        assertConfigDescribed();
    }

    // the config describe never answers within the probe's timeout: best effort, the probe still passes
    @Test
    @SuppressWarnings("unchecked")
    void timedOutConfigDescribeDoesNotRefuse() throws Exception {
        allGranted();
        KafkaFuture<Config> f = mock(KafkaFuture.class);
        when(f.get(anyLong(), any(java.util.concurrent.TimeUnit.class)))
                .thenThrow(new java.util.concurrent.TimeoutException("describeConfigs timed out"));
        when(f.get()).thenThrow(new AssertionError("describeConfigs must be awaited with a time bound"));
        cleanupPolicy(f);
        assertDoesNotThrow(() -> probe().probe());
        verify(f).get(anyLong(), any(java.util.concurrent.TimeUnit.class));
        assertConfigDescribed();
    }

    // a broker that returns no cleanup.policy entry at all is not an error
    @Test
    void configWithoutCleanupPolicyEntryDoesNotRefuse() {
        allGranted();
        cleanupPolicy(KafkaFuture.completedFuture(new Config(List.of())));
        assertDoesNotThrow(() -> probe().probe());
        assertConfigDescribed();
    }

    // a cleanup.policy entry whose value is null (e.g. sensitive/unreadable) is not an error
    @Test
    void cleanupPolicyEntryWithNullValueDoesNotRefuse() {
        allGranted();
        cleanupPolicy(KafkaFuture.completedFuture(new Config(List.of(new ConfigEntry("cleanup.policy", null)))));
        assertDoesNotThrow(() -> probe().probe());
        assertConfigDescribed();
    }

    /** Either describeConfigs overload satisfies the seam. */
    private void assertConfigDescribed() {
        assertTrue(org.mockito.Mockito.mockingDetails(admin).getInvocations().stream()
                .anyMatch(i -> i.getMethod().getName().equals("describeConfigs")),
                "the probe must describe the reply topic's configs");
    }
}
