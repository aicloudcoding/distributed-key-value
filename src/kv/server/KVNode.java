package kv.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import kv.cluster.ConsistentHashRing;
import kv.cluster.NodeInfo;
import kv.net.Connection;
import kv.net.Protocol;
import kv.storage.HybridClock;
import kv.storage.StorageEngine;
import kv.storage.VersionedValue;

/**
 * One node of the distributed key-value store.
 *
 * <p>Keys are partitioned over a consistent hash ring and each key is stored on N replicas.
 * Whichever node a client talks to acts as coordinator: it fans the request out to the
 * key's replicas and answers once W (writes) or R (reads) of them respond. With
 * {@code R + W > N} every read overlaps the latest successful write.
 *
 * <p>Fault tolerance:
 * <ul>
 *   <li><b>Failure detection</b> - peers are pinged periodically to learn when a failed
 *       replica is reachable again.</li>
 *   <li><b>Hinted handoff</b> - writes a down replica misses are kept by the coordinator and
 *       delivered once the replica is reachable again.</li>
 *   <li><b>Read repair</b> - replicas found holding stale data during a read are updated
 *       with the newest version.</li>
 *   <li><b>Durability</b> - each replica appends writes to a write-ahead log.</li>
 * </ul>
 */
public final class KVNode implements AutoCloseable {

    private record Hint(String key, VersionedValue value) {}

    private record Reply(NodeInfo node, VersionedValue value) {}

    private final NodeConfig config;
    private final NodeInfo self;
    private final ConsistentHashRing ring;
    private final StorageEngine storage;
    private final HybridClock clock = new HybridClock();
    private final List<NodeInfo> peers;
    private final Set<String> downNodes = ConcurrentHashMap.newKeySet();
    private final Map<String, Deque<Hint>> hints = new ConcurrentHashMap<>();
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "heartbeat");
        t.setDaemon(true);
        return t;
    });
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private ServerSocket server;
    private volatile boolean running;

    public KVNode(NodeConfig config) {
        this.config = config;
        this.self = config.self();
        this.ring = new ConsistentHashRing(config.cluster(), NodeConfig.VIRTUAL_NODES);
        this.peers = config.cluster().stream().filter(n -> !n.id().equals(self.id())).toList();
        this.storage = new StorageEngine(
                config.dataDir() == null ? null : config.dataDir().resolve(self.id() + ".wal"));
        storage.snapshot().values().forEach(v -> clock.observe(v.timestamp()));
    }

    public synchronized void start() throws IOException {
        server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(self.port()));
        running = true;
        Thread acceptor = new Thread(this::acceptLoop, "acceptor-" + self.id());
        acceptor.setDaemon(true);
        acceptor.start();
        heartbeat.scheduleWithFixedDelay(this::checkPeers, 0, config.heartbeatMillis(), TimeUnit.MILLISECONDS);
        log("listening on " + self.address() + " (N=" + config.replicas() + ", W=" + config.writeQuorum()
                + ", R=" + config.readQuorum() + ", peers=" + peers + ")");
    }

    @Override
    public synchronized void close() throws IOException {
        running = false;
        heartbeat.shutdownNow();
        if (server != null) server.close();
        for (Socket s : clients) {
            try {
                s.close();
            } catch (IOException ignored) {
                // closing anyway
            }
        }
        workers.shutdownNow();
        storage.close();
    }

    public NodeInfo info() {
        return self;
    }

    // ---------------------------------------------------------------- networking

    private void acceptLoop() {
        while (running) {
            try {
                Socket socket = server.accept();
                clients.add(socket);
                workers.submit(() -> serve(socket));
            } catch (IOException e) {
                if (running) log("accept failed: " + e.getMessage());
            }
        }
    }

    private void serve(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                out.write(handle(line.strip()));
                out.write('\n');
                out.flush();
            }
        } catch (SocketException ignored) {
            // client went away or node is shutting down
        } catch (IOException e) {
            if (running) log("connection error: " + e.getMessage());
        } finally {
            clients.remove(socket);
        }
    }

    String handle(String line) {
        String[] p = line.split(" ");
        try {
            return switch (p[0].toUpperCase()) {
                case "GET" -> {
                    requireArgs(p, 2);
                    yield coordinateGet(p[1]);
                }
                case "PUT" -> {
                    requireArgs(p, 3);
                    yield coordinatePut(p[1], Protocol.decodeValue(p[2]));
                }
                case "DEL" -> {
                    requireArgs(p, 2);
                    yield coordinatePut(p[1], null);
                }
                case "RGET" -> {
                    requireArgs(p, 2);
                    VersionedValue v = storage.get(p[1]);
                    yield v == null ? "NOT_FOUND"
                            : "VERSIONED " + v.timestamp() + " " + v.nodeId() + " " + Protocol.encodeValue(v.value());
                }
                case "RPUT" -> {
                    requireArgs(p, 5);
                    long ts = Long.parseLong(p[2]);
                    clock.observe(ts);
                    storage.put(p[1], new VersionedValue(Protocol.decodeValue(p[4]), ts, p[3]));
                    yield "OK";
                }
                case "PING" -> "PONG";
                case "STATUS" -> status();
                default -> "ERROR unknown command " + p[0];
            };
        } catch (IllegalArgumentException e) {
            return "ERROR " + e.getMessage();
        } catch (RuntimeException e) {
            log("failed to handle '" + line + "': " + e);
            return "ERROR internal error: " + e.getMessage();
        }
    }

    private static void requireArgs(String[] parts, int count) {
        if (parts.length != count) {
            throw new IllegalArgumentException("wrong number of arguments for " + parts[0]);
        }
    }

    private String status() {
        Set<String> up = new TreeSet<>();
        up.add(self.id());
        peers.stream().map(NodeInfo::id).filter(id -> !downNodes.contains(id)).forEach(up::add);
        int pendingHints = hints.values().stream().mapToInt(Deque::size).sum();
        return "STATUS " + self.id() + " keys=" + storage.size()
                + " up=" + String.join(",", up)
                + " down=" + String.join(",", new TreeSet<>(downNodes))
                + " hints=" + pendingHints;
    }

    // ---------------------------------------------------------------- coordination

    private String coordinatePut(String key, byte[] value) {
        Protocol.validateKey(key);
        VersionedValue v = new VersionedValue(value, clock.next(), self.id());
        List<CompletableFuture<NodeInfo>> acks = new ArrayList<>();
        for (NodeInfo replica : ring.preferenceList(key, config.replicas())) {
            acks.add(replicate(replica, key, v));
        }
        int got = awaitQuorum(acks, config.writeQuorum()).size();
        if (got >= config.writeQuorum()) {
            return "OK";
        }
        return "ERROR write quorum not reached (" + got + "/" + config.writeQuorum()
                + " acks); missed replicas will receive it via hinted handoff";
    }

    /** Writes to one replica; completes with the replica on success or {@code null} on failure. */
    private CompletableFuture<NodeInfo> replicate(NodeInfo replica, String key, VersionedValue v) {
        if (replica.equals(self)) {
            storage.put(key, v);
            return CompletableFuture.completedFuture(self);
        }
        // Peers believed down are still tried: requests run concurrently and the coordinator
        // returns as soon as a quorum answers, so a dead replica adds no latency.
        return CompletableFuture.supplyAsync(() -> {
            try {
                String r = send(replica, rput(key, v));
                if ("OK".equals(r)) return replica;
                log("replica " + replica.id() + " rejected write: " + r);
            } catch (IOException e) {
                markDown(replica, e);
            }
            addHint(replica, key, v);
            return null;
        }, workers);
    }

    private String coordinateGet(String key) {
        Protocol.validateKey(key);
        List<CompletableFuture<Reply>> futures = new ArrayList<>();
        for (NodeInfo replica : ring.preferenceList(key, config.replicas())) {
            futures.add(readFrom(replica, key));
        }
        List<Reply> replies = awaitQuorum(futures, config.readQuorum());
        if (replies.size() < config.readQuorum()) {
            return "ERROR read quorum not reached (" + replies.size() + "/" + config.readQuorum() + " replies)";
        }
        VersionedValue newest = null;
        for (Reply r : replies) {
            newest = VersionedValue.newest(newest, r.value());
        }
        scheduleReadRepair(key, futures, newest);
        if (newest == null || newest.isTombstone()) {
            return "NOT_FOUND";
        }
        return "VALUE " + Protocol.encodeValue(newest.value());
    }

    /** Reads from one replica; completes with a reply, or {@code null} if it could not answer. */
    private CompletableFuture<Reply> readFrom(NodeInfo replica, String key) {
        if (replica.equals(self)) {
            return CompletableFuture.completedFuture(new Reply(self, storage.get(key)));
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                String[] r = send(replica, "RGET " + key).split(" ");
                if (r[0].equals("NOT_FOUND")) return new Reply(replica, null);
                if (r[0].equals("VERSIONED") && r.length == 4) {
                    return new Reply(replica,
                            new VersionedValue(Protocol.decodeValue(r[3]), Long.parseLong(r[1]), r[2]));
                }
                log("unexpected read reply from " + replica.id() + ": " + String.join(" ", r));
            } catch (IOException e) {
                markDown(replica, e);
            }
            return null;
        }, workers);
    }

    /** Once every replica has answered, push the newest version to any that are behind. */
    private void scheduleReadRepair(String key, List<CompletableFuture<Reply>> futures, VersionedValue newest) {
        if (newest == null) return;
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .orTimeout(config.timeoutMillis() * 2L, TimeUnit.MILLISECONDS)
                .whenComplete((ignored, error) -> {
                    for (CompletableFuture<Reply> f : futures) {
                        Reply r = f.getNow(null);
                        if (r != null && !newest.sameVersion(r.value())) {
                            log("read repair: " + key + " on " + r.node().id());
                            replicate(r.node(), key, newest);
                        }
                    }
                });
    }

    /**
     * Waits until {@code needed} futures have produced non-null results, all futures have
     * finished, or the timeout passes, then returns whatever results arrived.
     */
    private <T> List<T> awaitQuorum(List<CompletableFuture<T>> futures, int needed) {
        List<T> results = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger finished = new AtomicInteger();
        CompletableFuture<Void> done = new CompletableFuture<>();
        for (CompletableFuture<T> f : futures) {
            f.whenComplete((value, error) -> {
                if (error == null && value != null) {
                    results.add(value);
                    if (results.size() >= needed) done.complete(null);
                }
                if (finished.incrementAndGet() == futures.size()) done.complete(null);
            });
        }
        try {
            done.get(config.timeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException | ExecutionException ignored) {
            // fall through with the replies gathered so far
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (results) {
            return new ArrayList<>(results);
        }
    }

    // ---------------------------------------------------------------- failure handling

    private void checkPeers() {
        for (NodeInfo peer : peers) {
            workers.submit(() -> {
                try {
                    if (!"PONG".equals(send(peer, "PING"))) throw new IOException("bad ping reply");
                    if (downNodes.remove(peer.id())) log(peer.id() + " is back up");
                    deliverHints(peer);
                } catch (IOException e) {
                    markDown(peer, e);
                }
            });
        }
    }

    private void markDown(NodeInfo peer, IOException cause) {
        if (downNodes.add(peer.id())) {
            log(peer.id() + " marked down: " + cause.getMessage());
        }
    }

    private void addHint(NodeInfo replica, String key, VersionedValue v) {
        hints.computeIfAbsent(replica.id(), id -> new ConcurrentLinkedDeque<>()).add(new Hint(key, v));
    }

    private void deliverHints(NodeInfo peer) throws IOException {
        Deque<Hint> queue = hints.get(peer.id());
        if (queue == null || queue.isEmpty()) return;
        synchronized (queue) {
            int delivered = 0;
            Hint h;
            while ((h = queue.poll()) != null) {
                try {
                    send(peer, rput(h.key(), h.value()));
                    delivered++;
                } catch (IOException e) {
                    queue.addFirst(h);
                    throw e;
                }
            }
            log("delivered " + delivered + " hinted write(s) to " + peer.id());
        }
    }

    private static String rput(String key, VersionedValue v) {
        return "RPUT " + key + " " + v.timestamp() + " " + v.nodeId() + " " + Protocol.encodeValue(v.value());
    }

    private String send(NodeInfo node, String line) throws IOException {
        return Connection.send(node.host(), node.port(), config.timeoutMillis(), line);
    }

    private void log(String msg) {
        System.err.println("[" + self.id() + "] " + msg);
    }

    /** Ids of peers this node currently believes are down (for tests and diagnostics). */
    public Set<String> downPeers() {
        return downNodes.stream().collect(Collectors.toUnmodifiableSet());
    }
}
