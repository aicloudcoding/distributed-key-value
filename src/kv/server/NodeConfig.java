package kv.server;

import java.nio.file.Path;
import java.util.List;

import kv.cluster.NodeInfo;

/**
 * Configuration for one node.
 *
 * @param self            this node
 * @param cluster         every node in the cluster, including {@code self}
 * @param replicas        N: copies kept of each key
 * @param writeQuorum     W: replica acks needed before a write succeeds
 * @param readQuorum      R: replica replies needed before a read succeeds
 * @param dataDir         directory for the write-ahead log, or {@code null} for memory only
 * @param timeoutMillis   network timeout for node-to-node requests
 * @param heartbeatMillis interval between failure-detector pings
 */
public record NodeConfig(
        NodeInfo self,
        List<NodeInfo> cluster,
        int replicas,
        int writeQuorum,
        int readQuorum,
        Path dataDir,
        int timeoutMillis,
        int heartbeatMillis) {

    public static final int VIRTUAL_NODES = 128;

    public NodeConfig {
        cluster = List.copyOf(cluster);
        if (cluster.stream().noneMatch(n -> n.id().equals(self.id()))) {
            throw new IllegalArgumentException("Cluster list must include this node (" + self.id() + ")");
        }
        if (cluster.stream().map(NodeInfo::id).distinct().count() != cluster.size()) {
            throw new IllegalArgumentException("Node ids must be unique");
        }
        replicas = Math.min(replicas, cluster.size());
        if (replicas < 1 || writeQuorum < 1 || readQuorum < 1
                || writeQuorum > replicas || readQuorum > replicas) {
            throw new IllegalArgumentException(
                    "Need 1 <= W,R <= N (N=" + replicas + ", W=" + writeQuorum + ", R=" + readQuorum + ")");
        }
    }

    public static NodeConfig defaults(NodeInfo self, List<NodeInfo> cluster) {
        int n = Math.min(3, cluster.size());
        int quorum = n / 2 + 1;
        return new NodeConfig(self, cluster, n, quorum, quorum, null, 1000, 500);
    }

    public NodeConfig withDataDir(Path dir) {
        return new NodeConfig(self, cluster, replicas, writeQuorum, readQuorum, dir, timeoutMillis, heartbeatMillis);
    }
}
