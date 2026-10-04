package kv.cluster;

import java.util.ArrayList;
import java.util.List;

/** Identity and network address of one node in the cluster. */
public record NodeInfo(String id, String host, int port) {

    public String address() {
        return host + ":" + port;
    }

    /** Parses {@code id=host:port}. */
    public static NodeInfo parse(String spec) {
        int eq = spec.indexOf('=');
        if (eq <= 0) {
            throw new IllegalArgumentException("Expected id=host:port but got: " + spec);
        }
        String id = spec.substring(0, eq).trim();
        String[] hostPort = parseHostPort(spec.substring(eq + 1).trim());
        return new NodeInfo(id, hostPort[0], Integer.parseInt(hostPort[1]));
    }

    /** Parses a comma separated list of {@code id=host:port} entries. */
    public static List<NodeInfo> parseList(String specs) {
        List<NodeInfo> nodes = new ArrayList<>();
        for (String spec : specs.split(",")) {
            if (!spec.isBlank()) {
                nodes.add(parse(spec));
            }
        }
        return nodes;
    }

    public static String[] parseHostPort(String hostPort) {
        int colon = hostPort.lastIndexOf(':');
        if (colon <= 0) {
            throw new IllegalArgumentException("Expected host:port but got: " + hostPort);
        }
        return new String[] {hostPort.substring(0, colon), hostPort.substring(colon + 1)};
    }

    @Override
    public String toString() {
        return id + "=" + address();
    }
}
