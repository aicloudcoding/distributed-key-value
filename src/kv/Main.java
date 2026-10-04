package kv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kv.client.KVClient;
import kv.cluster.NodeInfo;
import kv.server.KVNode;
import kv.server.NodeConfig;

/** Command-line entry point: run a node, an interactive client, or a local demo cluster. */
public final class Main {

    private static final String USAGE = """
            Usage:
              node   --id <id> --cluster <id=host:port,...> [--replicas N] [--w W] [--r R]
                     [--data-dir DIR] [--timeout MS] [--heartbeat MS]
                  Run one node. --cluster lists every node, including this one.

              client --nodes <host:port,...>
                  Interactive shell against a running cluster.

              demo   [--nodes COUNT] [--base-port PORT] [--data-dir DIR]
                  Start a local cluster in this process and open a shell to it,
                  with extra commands to stop and restart nodes.
            """;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.print(USAGE);
            return;
        }
        Map<String, String> opts = parseOptions(Arrays.copyOfRange(args, 1, args.length));
        switch (args[0]) {
            case "node" -> runNode(opts);
            case "client" -> runClient(opts);
            case "demo" -> runDemo(opts);
            default -> {
                System.err.print(USAGE);
                System.exit(1);
            }
        }
    }

    private static void runNode(Map<String, String> opts) throws IOException, InterruptedException {
        List<NodeInfo> cluster = NodeInfo.parseList(require(opts, "cluster"));
        String id = require(opts, "id");
        NodeInfo self = cluster.stream().filter(n -> n.id().equals(id)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("--id " + id + " is not in --cluster"));
        NodeConfig d = NodeConfig.defaults(self, cluster);
        int n = intOpt(opts, "replicas", d.replicas());
        int quorum = Math.min(n, cluster.size()) / 2 + 1;
        NodeConfig config = new NodeConfig(self, cluster, n,
                intOpt(opts, "w", quorum),
                intOpt(opts, "r", quorum),
                opts.containsKey("data-dir") ? Path.of(opts.get("data-dir")) : null,
                intOpt(opts, "timeout", d.timeoutMillis()),
                intOpt(opts, "heartbeat", d.heartbeatMillis()));
        KVNode node = new KVNode(config);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                node.close();
            } catch (IOException ignored) {
                // exiting
            }
        }));
        // Server threads are daemons, so park here until the process is terminated.
        Thread.currentThread().join();
    }

    private static void runClient(Map<String, String> opts) throws IOException {
        List<String> nodes = List.of(require(opts, "nodes").split(","));
        try (KVClient client = new KVClient(nodes)) {
            shell(client, null);
        }
    }

    private static void runDemo(Map<String, String> opts) throws IOException {
        int count = intOpt(opts, "nodes", 3);
        int basePort = intOpt(opts, "base-port", 7001);
        Path dataDir = opts.containsKey("data-dir") ? Path.of(opts.get("data-dir")) : null;
        List<NodeInfo> cluster = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            cluster.add(new NodeInfo("n" + (i + 1), "localhost", basePort + i));
        }
        Map<String, KVNode> running = new HashMap<>();
        Demo demo = new Demo(cluster, dataDir, running);
        for (NodeInfo n : cluster) demo.start(n.id());

        List<String> addresses = cluster.stream().map(NodeInfo::address).toList();
        try (KVClient client = new KVClient(addresses)) {
            shell(client, demo);
        } finally {
            for (KVNode node : running.values()) node.close();
        }
    }

    /** Lets the demo shell stop and restart in-process nodes to simulate failures. */
    private record Demo(List<NodeInfo> cluster, Path dataDir, Map<String, KVNode> running) {
        void start(String id) throws IOException {
            if (running.containsKey(id)) {
                System.out.println(id + " is already running");
                return;
            }
            NodeInfo self = cluster.stream().filter(n -> n.id().equals(id)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("unknown node " + id));
            KVNode node = new KVNode(NodeConfig.defaults(self, cluster).withDataDir(dataDir));
            node.start();
            running.put(id, node);
        }

        void stop(String id) throws IOException {
            KVNode node = running.remove(id);
            if (node == null) {
                System.out.println(id + " is not running");
                return;
            }
            node.close();
            System.out.println("stopped " + id);
        }
    }

    private static void shell(KVClient client, Demo demo) throws IOException {
        System.out.println("Commands: put <key> <value>, get <key>, del <key>, status"
                + (demo != null ? ", stop <node>, start <node>" : "") + ", quit");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        while (true) {
            System.out.print("kv> ");
            System.out.flush();
            String line = in.readLine();
            if (line == null) return;
            line = line.strip();
            if (line.isEmpty()) continue;
            String[] p = line.split("\\s+", 3);
            try {
                switch (p[0].toLowerCase()) {
                    case "put" -> {
                        if (p.length < 3) throw new IllegalArgumentException("usage: put <key> <value>");
                        client.put(p[1], p[2]);
                        System.out.println("OK");
                    }
                    case "get" -> {
                        if (p.length < 2) throw new IllegalArgumentException("usage: get <key>");
                        System.out.println(client.getString(p[1]).orElse("(not found)"));
                    }
                    case "del" -> {
                        if (p.length < 2) throw new IllegalArgumentException("usage: del <key>");
                        client.delete(p[1]);
                        System.out.println("OK");
                    }
                    case "status" -> System.out.println(client.status() + "  (via " + client.currentNode() + ")");
                    case "stop", "start" -> {
                        if (demo == null || p.length < 2) throw new IllegalArgumentException("usage: " + p[0] + " <node>");
                        if (p[0].equalsIgnoreCase("stop")) demo.stop(p[1]);
                        else demo.start(p[1]);
                    }
                    case "quit", "exit" -> {
                        return;
                    }
                    default -> System.out.println("unknown command: " + p[0]);
                }
            } catch (IllegalArgumentException | IOException e) {
                System.out.println("error: " + e.getMessage());
            }
        }
    }

    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException("Expected --option value, got: " + args[i]);
            }
            opts.put(args[i].substring(2), args[++i]);
        }
        return opts;
    }

    private static String require(Map<String, String> opts, String name) {
        String v = opts.get(name);
        if (v == null) throw new IllegalArgumentException("Missing required option --" + name);
        return v;
    }

    private static int intOpt(Map<String, String> opts, String name, int def) {
        return opts.containsKey(name) ? Integer.parseInt(opts.get(name)) : def;
    }
}
