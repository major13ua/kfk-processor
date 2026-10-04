package xme.common.kfkprocessor.requestreply.autoconfig;

/** Startup refusal; message is plain language and contains the error code. */
public class ConfigurationRefusedException extends RuntimeException {
    private final String code;

    public ConfigurationRefusedException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() { return code; }
}
