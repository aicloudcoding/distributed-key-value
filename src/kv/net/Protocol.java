package kv.net;

import java.util.Base64;

/**
 * Line-based text protocol spoken over TCP. One request line, one response line.
 * Keys must not contain whitespace; values are Base64 encoded so they may hold any bytes.
 *
 * <pre>
 * Client commands (any node can coordinate):
 *   GET &lt;key&gt;                      -> VALUE &lt;b64&gt; | NOT_FOUND | ERROR &lt;msg&gt;
 *   PUT &lt;key&gt; &lt;b64&gt;                -> OK | ERROR &lt;msg&gt;
 *   DEL &lt;key&gt;                      -> OK | ERROR &lt;msg&gt;
 *   PING                           -> PONG
 *   STATUS                         -> STATUS &lt;node&gt; keys=&lt;n&gt; up=&lt;ids&gt; down=&lt;ids&gt; hints=&lt;n&gt;
 *
 * Replica commands (node to node):
 *   RGET &lt;key&gt;                     -> VERSIONED &lt;ts&gt; &lt;node&gt; &lt;b64|-&gt; | NOT_FOUND
 *   RPUT &lt;key&gt; &lt;ts&gt; &lt;node&gt; &lt;b64|-&gt; -> OK
 * </pre>
 *
 * A {@code -} value denotes a tombstone (deleted key).
 */
public final class Protocol {

    public static final String TOMBSTONE = "-";

    private Protocol() {}

    public static String encodeValue(byte[] value) {
        return value == null ? TOMBSTONE : Base64.getEncoder().encodeToString(value);
    }

    public static byte[] decodeValue(String encoded) {
        return TOMBSTONE.equals(encoded) ? null : Base64.getDecoder().decode(encoded);
    }

    public static void validateKey(String key) {
        if (key == null || key.isEmpty() || key.chars().anyMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("Keys must be non-empty and contain no whitespace");
        }
    }
}
