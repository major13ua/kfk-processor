# Architecture Overview: Spring Boot Request-Reply Kafka Starter

This document provides a comprehensive architectural specification for building a reusable Spring Boot starter / library (`spring-boot-starter-kafka-request-reply`) in **Java 25**, **Spring Boot 4.x**, and **Apache Kafka 4.2**. 

---

## 1. System Vision & Core Objectives

When implementing a Request-Reply pattern over Kafka (where a worker consumes requests, enriches data via external REST/gRPC/DB calls, and publishes replies to an outbound topic), traditional consumer loops suffer from:
1. **Uncontrolled upstream overload**: Rapid polling floods downstream HTTP services.
2. **Sequential batch lag compounding**: The 1st record taking 2s delays all subsequent records in the batch.
3. **Rebalance churn**: Rolling deployments disrupt all partitions across the consumer group.
4. **Duplicate responses**: Uncommitted offsets during rebalances cause re-execution.

### Objectives of the Library
- **Spring Boot 4 Auto-Configuration**: Simple annotation-driven or programmatic container setup (`@EnableKafkaRequestReply`).
- **Java 25 Native Concurrency**: Virtual Threads (`Executors.newVirtualThreadPerTaskExecutor()`) for non-blocking parallel HTTP/gRPC enrichment.
- **Polling-Layer Rate Limiting**: Ingestion throttle via `Bucket4j` with token compensation.
- **Multi-Topic Prioritization**: `CyclicBarrier` thread-per-consumer model to combine High-Priority and Low-Priority topics safely.
- **Transactional Exactly-Once**: Single atomic Kafka transaction wrapping outbound responses and consumer offset commits.
- **Zero-Rebalance Kubernetes StatefulSet Integration**: Static membership (`group.instance.id`).
- **Real-Time Latency Visibility**: Custom `Consistency Lag` Micrometer metrics.

---

## 2. High-Level Architecture Diagram

```
+-----------------------------------------------------------------------------------+
|                        SPRING BOOT REQUEST-REPLY WORKER                           |
|                                                                                   |
|  [Topic: requests.high] -> Consumer Thread 1 -\                                   |
|                                                +-> [CyclicBarrier Callback]       |
|  [Topic: requests.low]  -> Consumer Thread 2 -/          |                        |
|                                                          v                        |
|                                                [Bucket4j Rate Limiter]            |
|                                                          |                        |
|                                                          v                        |
|                                              [Virtual Threads Pool]               |
|                                            (Async REST/gRPC Processing)           |
|                                                          |                        |
|                                                          v                        |
|                                            [Transactional Kafka Producer]         |
|                                            - Publish Responses                    |
|                                            - Send Consumer Offsets to Tx          |
|                                            - Commit Transaction                   |
+-----------------------------------------------------------------------------------+
                                                           |
                                                           v
                                                [Topic: replies.outbound]
```

---

## 3. Core Architectural Modules

### A. Polling-Layer Rate Limiting (`Bucket4j`)
- **Problem**: Rate limiting *after* polling buffers records in memory, increasing GC pressure and risking `max.poll.interval.ms` timeouts.
- **Solution**: Rate-limit *before* `consumer.poll()`.
- **Algorithm**:
  1. Reserve `max.poll.records` tokens from a `Bucket4j` instance configured with `refillGreedy`.
  2. If tokens are missing, block/park the consumer thread until tokens refill.
  3. Execute `consumer.poll(pollTimeout)`.
  4. **Token Compensation**: Calculate `unused = maxPollRecords - records.count()`. If `unused > 0`, return `unused` tokens back to the bucket immediately.

### B. Multi-Topic Synchronization (`CyclicBarrier`)
- **Problem**: `KafkaConsumer` is **not thread-safe** and throws `ConcurrentModificationException`. However, we want to poll High-Priority and Low-Priority topics in parallel and merge their batches under a single global rate limit.
- **Solution**:
  1. Instantiate one `KafkaConsumer` per topic, each running on its dedicated Virtual or Platform Thread.
  2. Each consumer polls its topic using the rate-limited poll method and pushes records into a shared `ConcurrentLinkedQueue`.
  3. Both consumer threads call `barrier.await()`.
  4. The `CyclicBarrier` callback processes the combined batch (sorting by priority), dispatches to Virtual Threads, sends responses, commits offsets in a transaction, and unblocks both consumer threads.

### C. Transactional Exactly-Once Delivery
- **Mechanism**:
  ```java
  producer.beginTransaction();
  for (var reply : replies) {
      producer.send(reply);
  }
  producer.sendOffsetsToTransaction(offsetsAndMetadata, consumerGroupId);
  producer.commitTransaction();
  ```
- **Rules**:
  - `transactional.id` must be unique per pod (e.g. `request-reply-worker-${HOSTNAME}`).
  - Shared Kafka cluster for request topics, offset topic (`__consumer_offsets`), and reply topics.
  - Downstream consumers reading replies must configure `isolation.level = read_committed`.

### D. Static Membership & Zero-Rebalance Rolling Deployments
- **Kubernetes Configuration**: StatefulSet with deterministic hostnames (`pod-0`, `pod-1`).
- **Kafka Configuration**:
  - `group.instance.id = System.getenv("HOSTNAME")`
  - `session.timeout.ms = 45000` (> Pod restart time)
- **Benefit**: Rolling updates replace pods without triggering consumer group rebalances across surviving pods.

### E. Consistency Lag Metric
- **Definition**: `Consistency Lag = System.currentTimeMillis() - record.timestamp()`
- Measured immediately **after** the transaction commits.
- Published to Micrometer (`MeterRegistry`) as a distribution summary/histogram.

---

## 4. Spring Boot Auto-Configuration API

The library will export a Spring Boot Auto-Configuration module:

```yaml
spring:
  kafka:
    request-reply:
      enabled: true
      group-id: "order-processing-group"
      reply-topic: "orders.responses"
      rate-limit:
        rps: 100
        greedy-refill-period: "1s"
      topics:
        high-priority: "orders.requests.high"
        low-priority: "orders.requests.low"
      transactional-id-prefix: "tx-order-worker-"
```

Developers define a custom request handler bean:

```java
@Component
public class OrderRequestHandler implements RequestReplyHandler<String, OrderRequest, OrderResponse> {
    @Override
    public OrderResponse handle(String key, OrderRequest request, Map<String, byte[]> headers) {
        // Enriched via external HTTP / DB call
        return orderService.process(request);
    }
}
```

---

## 5. Summary of Tech Stack Specifications

| Dimension | Specification |
| :--- | :--- |
| **JDK** | Java 25 (Virtual Threads, Records, Sequenced Collections, Scoped Values) |
| **Spring Framework** | Spring Boot 4.x / Spring 6.x+ |
| **Kafka Client** | Apache Kafka Clients 4.2+ |
| **Rate Limiter** | Bucket4j 8.x / 9.x |
| **Deployment Target** | Kubernetes StatefulSet (Static Membership) |
