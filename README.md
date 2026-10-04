# Distributed Key-Value Store

A small Dynamo-style distributed key-value store in plain Java (21+, no dependencies).

## Features

- **Partitioning** – keys are spread over a consistent hash ring with 128 virtual nodes per server.
- **Replication** – each key lives on `N` nodes (default 3).
- **Tunable quorums** – writes need `W` acks and reads need `R` replies (default majority, so `R + W > N`
  and every read sees the latest successful write).
- **Any node coordinates** – clients may talk to any node; the client library fails over automatically.
- **Conflict resolution** – last-writer-wins using a hybrid logical clock; deletes are tombstones.
- **Failure detection** – nodes ping each other; `STATUS` shows which peers are up or down.
- **Hinted handoff** – writes a down replica misses are queued and delivered when it comes back.
- **Read repair** – stale replicas discovered during reads are updated in the background.
- **Durability** – each node appends to a write-ahead log (`--data-dir`) and replays it on restart.

## Build and test

```bash
./build.sh      # compiles to out/production
./test.sh       # builds and runs the cluster test suite
```

The project is also an IntelliJ IDEA module (`src/` sources, `test/` tests).

## Try it

Quickest: a 3-node cluster in one process with an interactive shell.

```bash
./kv.sh demo
kv> put user:1 Alice Smith
kv> get user:1
kv> stop n1          # simulate a crash
kv> get user:1       # still served by the other replicas
kv> put user:2 Bob   # n1 misses this; a hint is kept for it
kv> start n1         # n1 rejoins and receives the hinted write
kv> status
```

As real separate processes (run each node in its own terminal or machine):

```bash
C=n1=localhost:7001,n2=localhost:7002,n3=localhost:7003
./kv.sh node --id n1 --cluster $C --data-dir data
./kv.sh node --id n2 --cluster $C --data-dir data
./kv.sh node --id n3 --cluster $C --data-dir data

./kv.sh client --nodes localhost:7001,localhost:7002,localhost:7003
```

Node options: `--replicas N`, `--w W`, `--r R`, `--data-dir DIR`, `--timeout MS`, `--heartbeat MS`.

## Using it from Java

```java
try (KVClient kv = new KVClient(List.of("localhost:7001", "localhost:7002"))) {
    kv.put("color", "blue");
    Optional<String> color = kv.getString("color");
    kv.delete("color");
}
```

## Wire protocol

Line-based text over TCP; values are Base64 so any bytes are allowed, keys may not contain whitespace.

| Request                          | Response                                   |
|----------------------------------|--------------------------------------------|
| `GET <key>`                      | `VALUE <b64>` / `NOT_FOUND` / `ERROR <msg>`|
| `PUT <key> <b64>`                | `OK` / `ERROR <msg>`                       |
| `DEL <key>`                      | `OK` / `ERROR <msg>`                       |
| `STATUS`                         | `STATUS <id> keys=.. up=.. down=.. hints=..` |
| `PING`                           | `PONG`                                     |
| `RGET <key>` *(internal)*        | `VERSIONED <ts> <node> <b64\|->` / `NOT_FOUND` |
| `RPUT <key> <ts> <node> <b64\|->` *(internal)* | `OK`                         |

e.g. `printf 'PUT k aGk=\nGET k\n' | nc localhost 7001`

## Layout

```
src/kv/Main.java                     CLI: node, client, demo
src/kv/cluster/ConsistentHashRing    partitioning + replica selection
src/kv/server/KVNode                 server, coordinator, quorums, hints, read repair, failure detector
src/kv/storage/StorageEngine         in-memory map + write-ahead log
src/kv/storage/VersionedValue        last-writer-wins versions and tombstones
src/kv/client/KVClient               client library with failover
test/kv/ClusterTest                  end-to-end tests on real sockets
```

## Limitations

- Cluster membership is static (set at start-up); nodes cannot join or leave dynamically.
- Hints are held in memory, so they are lost if the coordinator itself restarts.
- No background anti-entropy (Merkle trees); replicas converge via hints and read repair.
- Last-writer-wins can drop one of two truly concurrent writes (no vector clocks).
- The write-ahead log is never compacted, and traffic is unauthenticated plain TCP.
