package xme.common.kfkprocessor.requestreply.api;

/** Successful reply: correlation id and Request Key echoed unchanged, plus the Handler result. */
public record Reply<K, RES>(String correlationId, K requestKey, RES data) {
}
