package kv.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import kv.cluster.NodeInfo;
import kv.net.Connection;
import kv.net.Protocol;

/**
 * Client for the key-value store. Any node can serve any request, so the client simply
 * tries the configured nodes in turn and fails over when one is unreachable.
 */
public final class KVClient implements AutoCloseable {

    private final List<String[]> nodes = new ArrayList<>();
    private final int timeoutMillis;
    private Connection connection;
    private int current;

    /** @param addresses {@code host:port} of one or more nodes */
    public KVClient(List<String> addresses, int timeoutMillis) {
        if (addresses.isEmpty()) throw new IllegalArgumentException("At least one node address is required");
        for (String a : addresses) nodes.add(NodeInfo.parseHostPort(a));
        this.timeoutMillis = timeoutMillis;
    }

    public KVClient(List<String> addresses) {
        this(addresses, 5000);
    }

    public Optional<byte[]> get(String key) throws IOException {
        Protocol.validateKey(key);
        String r = request("GET " + key);
        if (r.equals("NOT_FOUND")) return Optional.empty();
        if (r.startsWith("VALUE ")) return Optional.of(Protocol.decodeValue(r.substring(6)));
        throw new KVException(r);
    }

    public Optional<String> getString(String key) throws IOException {
        return get(key).map(b -> new String(b, StandardCharsets.UTF_8));
    }

    public void put(String key, byte[] value) throws IOException {
        Protocol.validateKey(key);
        expectOk(request("PUT " + key + " " + Protocol.encodeValue(value)));
    }

    public void put(String key, String value) throws IOException {
        put(key, value.getBytes(StandardCharsets.UTF_8));
    }

    public void delete(String key) throws IOException {
        Protocol.validateKey(key);
        expectOk(request("DEL " + key));
    }

    public String status() throws IOException {
        return request("STATUS");
    }

    private static void expectOk(String r) throws KVException {
        if (!r.equals("OK")) throw new KVException(r);
    }

    /** Sends a request, failing over to the next node on network errors. */
    private synchronized String request(String line) throws IOException {
        IOException last = null;
        for (int attempt = 0; attempt < nodes.size(); attempt++) {
            try {
                if (connection == null) {
                    String[] hp = nodes.get(current);
                    connection = new Connection(hp[0], Integer.parseInt(hp[1]), timeoutMillis);
                }
                return connection.request(line);
            } catch (IOException e) {
                last = e;
                closeQuietly();
                current = (current + 1) % nodes.size();
            }
        }
        throw new IOException("No node reachable", last);
    }

    /** The node the client is currently talking to. */
    public synchronized String currentNode() {
        String[] hp = nodes.get(current);
        return hp[0] + ":" + hp[1];
    }

    private void closeQuietly() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException ignored) {
                // already broken
            }
            connection = null;
        }
    }

    @Override
    public synchronized void close() {
        closeQuietly();
    }

    /** The cluster answered with an error, e.g. a quorum could not be reached. */
    public static final class KVException extends IOException {
        public KVException(String response) {
            super(response.startsWith("ERROR ") ? response.substring(6) : response);
        }
    }
}
