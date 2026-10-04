package kv;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BooleanSupplier;

import kv.client.KVClient;
import kv.cluster.ConsistentHashRing;
import kv.cluster.NodeInfo;
import kv.server.KVNode;
import kv.server.NodeConfig;

/**
 * Dependency-free test suite: spins up real nodes on localhost and exercises replication,
 * failover, hinted handoff, read repair and persistence. Run with {@code ./test.sh}.
 */
public final class ClusterTest {

    private static int passed;
    private static int failed;

    public static void main(String[] args) throws Exception {
        run("ring spreads keys and returns distinct replicas", ClusterTest::ringDistribution);
        run("put/get/delete through any node", ClusterTest::basicOperations);
        run("binary and multi-word values round-trip", ClusterTest::valueEncoding);
        run("survives one node failure", ClusterTest::singleFailure);
        run("hinted handoff catches up a restarted node", ClusterTest::hintedHandoff);
        run("write fails when quorum is lost", ClusterTest::quorumLoss);
        run("data survives a full restart via the WAL", ClusterTest::persistence);
        System.out.println();
        System.out.println(passed + " passed, " + failed + " failed");
        System.exit(failed == 0 ? 0 : 1);
    }

    // ---------------------------------------------------------------- tests

    static void ringDistribution() {
        List<NodeInfo> nodes = List.of(node("a", 1), node("b", 2), node("c", 3), node("d", 4));
        ConsistentHashRing ring = new ConsistentHashRing(nodes, NodeConfig.VIRTUAL_NODES);
        Map<String, Integer> primaries = new HashMap<>();
        for (int i = 0; i < 10_000; i++) {
            List<NodeInfo> prefs = ring.preferenceList("key" + i, 3);
            check(prefs.size() == 3, "expected 3 replicas");
            check(prefs.stream().distinct().count() == 3, "replicas must be distinct");
            primaries.merge(prefs.get(0).id(), 1, Integer::sum);
        }
        for (int count : primaries.values()) {
            check(count > 1500 && count < 3500, "uneven distribution: " + primaries);
        }
    }

    static void basicOperations() throws Exception {
        try (TestCluster c = new TestCluster(3, null)) {
            try (KVClient a = c.clientFor(0); KVClient b = c.clientFor(1); KVClient d = c.clientFor(2)) {
                a.put("color", "blue");
                checkEquals("blue", b.getString("color"));
                d.put("color", "green");
                checkEquals("green", a.getString("color"));
                b.delete("color");
                check(d.getString("color").isEmpty(), "deleted key should be gone");
                check(a.getString("missing").isEmpty(), "missing key should be empty");
            }
        }
    }

    static void valueEncoding() throws Exception {
        try (TestCluster c = new TestCluster(3, null); KVClient client = c.client()) {
            byte[] bin = {0, 1, 2, (byte) 0xFF, '\n', ' '};
            client.put("bin", bin);
            check(java.util.Arrays.equals(bin, client.get("bin").orElseThrow()), "binary mismatch");
            client.put("text", "hello distributed world\nline two");
            checkEquals("hello distributed world\nline two", client.getString("text"));
        }
    }

    static void singleFailure() throws Exception {
        try (TestCluster c = new TestCluster(3, null); KVClient client = c.client()) {
            for (int i = 0; i < 50; i++) client.put("k" + i, "v" + i);
            c.stop(0);
            for (int i = 0; i < 50; i++) checkEquals("v" + i, client.getString("k" + i));
            for (int i = 0; i < 50; i++) client.put("k" + i, "updated" + i);
            for (int i = 0; i < 50; i++) checkEquals("updated" + i, client.getString("k" + i));
        }
    }

    static void hintedHandoff() throws Exception {
        try (TestCluster c = new TestCluster(3, null)) {
            c.stop(2);
            try (KVClient client = c.clientFor(0)) {
                for (int i = 0; i < 20; i++) client.put("h" + i, "x" + i);
            }
            c.start(2);
            // With N=3 every node holds every key, so n3 alone must catch up via hints.
            waitFor(() -> c.node(2).downPeers().isEmpty() && keysOn(c, 2) == 20, 5000,
                    "restarted node did not receive hinted writes");
        }
    }

    static void quorumLoss() throws Exception {
        try (TestCluster c = new TestCluster(3, null); KVClient client = c.clientFor(0)) {
            client.put("before", "1");
            c.stop(1);
            c.stop(2);
            try {
                client.put("after", "2");
                throw new AssertionError("write should fail without a quorum");
            } catch (KVClient.KVException expected) {
                check(expected.getMessage().contains("quorum"), "unexpected error: " + expected.getMessage());
            }
            try {
                client.get("before");
                throw new AssertionError("read should fail without a quorum");
            } catch (KVClient.KVException expected) {
                check(expected.getMessage().contains("quorum"), "unexpected error: " + expected.getMessage());
            }
        }
    }

    static void persistence() throws Exception {
        Path dir = Files.createTempDirectory("kv-test");
        try (TestCluster c = new TestCluster(3, dir)) {
            try (KVClient client = c.client()) {
                client.put("durable", "yes");
                client.put("gone", "soon");
                client.delete("gone");
            }
            for (int i = 0; i < 3; i++) c.stop(i);
            for (int i = 0; i < 3; i++) c.start(i);
            try (KVClient client = c.client()) {
                checkEquals("yes", client.getString("durable"));
                check(client.getString("gone").isEmpty(), "tombstone should survive restart");
            }
        }
    }

    // ---------------------------------------------------------------- harness

    private static int keysOn(TestCluster c, int index) {
        try (KVClient client = c.clientFor(index)) {
            String status = client.status();
            String keys = status.split("keys=")[1].split(" ")[0];
            return Integer.parseInt(keys);
        } catch (IOException e) {
            return -1;
        }
    }

    static final class TestCluster implements AutoCloseable {
        final List<NodeInfo> infos = new ArrayList<>();
        final KVNode[] nodes;
        final Path dataDir;

        TestCluster(int size, Path dataDir) throws IOException {
            this.dataDir = dataDir;
            for (int i = 0; i < size; i++) infos.add(new NodeInfo("n" + (i + 1), "localhost", freePort()));
            nodes = new KVNode[size];
            for (int i = 0; i < size; i++) start(i);
        }

        void start(int i) throws IOException {
            NodeConfig d = NodeConfig.defaults(infos.get(i), infos);
            NodeConfig config = new NodeConfig(d.self(), d.cluster(), d.replicas(), d.writeQuorum(),
                    d.readQuorum(), dataDir, 500, 100);
            nodes[i] = new KVNode(config);
            nodes[i].start();
        }

        void stop(int i) throws IOException {
            nodes[i].close();
            nodes[i] = null;
        }

        KVNode node(int i) {
            return nodes[i];
        }

        KVClient client() {
            return new KVClient(infos.stream().map(NodeInfo::address).toList(), 3000);
        }

        KVClient clientFor(int i) {
            return new KVClient(List.of(infos.get(i).address()), 3000);
        }

        @Override
        public void close() throws IOException {
            for (int i = 0; i < nodes.length; i++) if (nodes[i] != null) stop(i);
        }
    }

    private static NodeInfo node(String id, int port) {
        return new NodeInfo(id, "localhost", port);
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void waitFor(BooleanSupplier condition, long timeoutMillis, String message) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        throw new AssertionError(message);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static void checkEquals(String expected, Optional<String> actual) {
        check(actual.isPresent() && expected.equals(actual.get()),
                "expected '" + expected + "' but got " + actual.orElse("(not found)"));
    }

    private interface TestBody {
        void run() throws Exception;
    }

    private static void run(String name, TestBody body) {
        try {
            body.run();
            passed++;
            System.out.println("PASS  " + name);
        } catch (Throwable t) {
            failed++;
            System.out.println("FAIL  " + name + ": " + t);
        }
    }
}
