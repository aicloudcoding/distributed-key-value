package kv.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Consistent hash ring with virtual nodes. Each physical node owns many points on the
 * ring so keys spread evenly and adding/removing a node only moves a small share of keys.
 */
public final class ConsistentHashRing {

    private final NavigableMap<Long, NodeInfo> ring = new TreeMap<>();
    private final int virtualNodes;
    private int nodeCount;

    public ConsistentHashRing(Collection<NodeInfo> nodes, int virtualNodes) {
        if (virtualNodes <= 0) {
            throw new IllegalArgumentException("virtualNodes must be positive");
        }
        this.virtualNodes = virtualNodes;
        for (NodeInfo node : nodes) {
            add(node);
        }
    }

    public synchronized void add(NodeInfo node) {
        for (int i = 0; i < virtualNodes; i++) {
            ring.put(hash(node.id() + "#" + i), node);
        }
        nodeCount++;
    }

    public synchronized void remove(NodeInfo node) {
        for (int i = 0; i < virtualNodes; i++) {
            ring.remove(hash(node.id() + "#" + i));
        }
        nodeCount--;
    }

    /**
     * Returns the first {@code count} distinct physical nodes found walking clockwise
     * from the key's position. The first entry is the key's primary owner.
     */
    public synchronized List<NodeInfo> preferenceList(String key, int count) {
        Set<NodeInfo> result = new LinkedHashSet<>();
        if (ring.isEmpty()) {
            return new ArrayList<>(result);
        }
        int wanted = Math.min(count, nodeCount);
        long h = hash(key);
        for (Map.Entry<Long, NodeInfo> e : ring.tailMap(h, true).entrySet()) {
            if (result.size() == wanted) break;
            result.add(e.getValue());
        }
        for (Map.Entry<Long, NodeInfo> e : ring.headMap(h, false).entrySet()) {
            if (result.size() == wanted) break;
            result.add(e.getValue());
        }
        return new ArrayList<>(result);
    }

    static long hash(String s) {
        try {
            byte[] d = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (d[i] & 0xFF);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
