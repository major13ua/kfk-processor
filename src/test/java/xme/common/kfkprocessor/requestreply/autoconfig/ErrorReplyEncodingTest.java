package xme.common.kfkprocessor.requestreply.autoconfig;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import xme.common.kfkprocessor.requestreply.api.ErrorCategory;
import xme.common.kfkprocessor.requestreply.api.ErrorReply;

/** F2 (review B8): the Error Reply body must be valid JSON whatever the correlation id and key contain. */
class ErrorReplyEncodingTest {

    @Test
    void bodyIsValidJsonForQuotesBackslashesNewlinesAndControlCharacters() {
        String corr = "a\"b\\c\nd\te\u0001f g";
        String key = "k\r\n\u0000\"";
        byte[] body = RequestReplyAutoConfiguration.encodeError(ErrorReply.of(ErrorCategory.TIMEOUT, corr, key));

        JsonNode json = JsonMapper.builder().build().readTree(new String(body, StandardCharsets.UTF_8));
        assertEquals(corr, json.get("correlation_id").asString());
        assertEquals(key, json.get("request_key").asString());
        assertEquals("timeout", json.get("category").asString());
    }
}
