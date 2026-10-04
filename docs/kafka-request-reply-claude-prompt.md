```text
TASK SPECIFICATION FOR CLAUDE CODE:

Create a production-ready, highly resilient Java 25 Spring Boot 4.x library (`spring-boot-starter-kafka-request-reply`) that implements an enterprise-grade Request-Reply worker on Apache Kafka 4.2, based on Nikolai Rudopas's "Кровь, пот и Kafka" architecture.

================================================================================
1. TECH STACK & PREREQUISITES
================================================================================
- Language: Java 25 (Utilize Records, Pattern Matching, Sequenced Collections, and Virtual Threads `Executors.newVirtualThreadPerTaskExecutor()`).
- Framework: Spring Boot 4.x / Spring Framework 7.x (or Spring 6.x / Spring Boot 3.x+ modern compatibility).
- Messaging: Apache Kafka Client 4.2+.
- Rate Limiting: Bucket4j 8.x / 9.x.
- Metrics: Micrometer (`MeterRegistry`).
- Testing: JUnit 5, Testcontainers for Apache Kafka 4.x.

================================================================================
2. CORE ARCHITECTURAL PATTERNS TO IMPLEMENT
================================================================================

A. Rate-Limited Polling Layer (Bucket4j with Token Compensation)
   - Do NOT rate-limit outbound requests in memory. Rate-limit directly at the Kafka polling layer.
   - Configure Bucket4j token bucket:
     * capacity = 2 * max.poll.records
     * refill = greedy refill (e.g. 100 tokens per second)
   - Before `consumer.poll(duration)`:
     * Call `bucket.asBlocking().consume(maxPollRecords)` to reserve tokens.
   - After `consumer.poll(duration)`:
     * Calculate `unusedTokens = maxPollRecords - records.count()`.
     * If `unusedTokens > 0`, return them to the bucket via `bucket.addTokens(unusedTokens)`.

B. Multi-Topic Synchronization Barrier (CyclicBarrier Thread-per-Consumer)
   - Support priority consumption across two request topics (e.g. High Priority vs Low Priority).
   - Because `KafkaConsumer` is NOT thread-safe, assign each topic to a dedicated consumer thread.
   - Both threads poll into a shared `ConcurrentLinkedQueue<ConsumerRecord<K, V>>`.
   - Use `java.util.concurrent.CyclicBarrier(2, barrierAction)` to synchronize batches.
   - The barrier callback processes the combined batch:
     1. Drain shared queue.
     2. Process records in parallel using Java 25 Virtual Threads (`Executors.newVirtualThreadPerTaskExecutor()`).
     3. Send responses and commit offsets atomically inside a single Kafka transaction.

C. Exactly-Once Delivery (Transactional Outbox & Offsets)
   - Bind outbound message sending and offset commits in a single Kafka transaction:
     ```java
     producer.beginTransaction();
     for (var replyRecord : replyRecords) {
         producer.send(replyRecord);
     }
     producer.sendOffsetsToTransaction(offsetsAndMetadata, consumerGroupId);
     producer.commitTransaction();
     ```
   - Ensure `transactional.id` is unique per pod (e.g., `prefix-${HOSTNAME}`).
   - Handle transaction aborts cleanly (`producer.abortTransaction()`) on processing failures.

D. Static Membership for Kubernetes StatefulSet Zero-Rebalance
   - Configure consumer properties:
     * `group.instance.id = System.getenv("HOSTNAME")`
     * `session.timeout.ms = 45000`
   - Prevents group-wide rebalances during rolling deployments in K8s StatefulSets.

E. Custom Metric: Consistency Lag
   - Calculate `Consistency Lag = System.currentTimeMillis() - record.timestamp()` for every record.
   - Record this latency to Micrometer (`MeterRegistry`) as a timer/distribution summary immediately AFTER offset commit transaction succeeds.

================================================================================
3. PROJECT STRUCTURE & CODE TO GENERATE
================================================================================

Please generate a modular Maven/Gradle library project structure containing:

1. `org.example.kafkarequestreply.annotation.EnableKafkaRequestReply`
   - Custom Spring annotation to activate the auto-configuration.

2. `org.example.kafkarequestreply.config.RequestReplyProperties`
   - `@ConfigurationProperties("spring.kafka.request-reply")` record holding topic names, rate limits, consumer group IDs, and transactional prefixes.

3. `org.example.kafkarequestreply.handler.RequestReplyHandler<K, REQ, RES>`
   - Functional interface for user-provided business processing.

4. `org.example.kafkarequestreply.consumer.RateLimitedKafkaConsumer`
   - Class wrapping `KafkaConsumer` with Bucket4j reservation and compensation logic.

5. `org.example.kafkarequestreply.container.PrioritySynchronizedConsumerContainer`
   - The core engine managing the consumer threads, `CyclicBarrier`, Virtual Thread execution, and Kafka transactions.

6. `org.example.kafkarequestreply.metrics.ConsistencyLagMetrics`
   - Micrometer integration for tracking processing latency.

7. `org.example.kafkarequestreply.config.RequestReplyAutoConfiguration`
   - Spring Boot auto-configuration bean definitions.

8. `README.md` and `application.yml` example.
9. Integration test using Testcontainers (`KafkaContainer`).
```
