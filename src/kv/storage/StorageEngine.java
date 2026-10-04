package kv.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import kv.net.Protocol;

/**
 * Local replica storage: an in-memory map backed by an optional append-only write-ahead
 * log. On start-up the log is replayed so a restarted node recovers its data.
 */
public final class StorageEngine implements AutoCloseable {

    private final Map<String, VersionedValue> data = new ConcurrentHashMap<>();
    private final BufferedWriter wal;

    /** @param walFile log file to persist to, or {@code null} for memory-only storage */
    public StorageEngine(Path walFile) {
        try {
            if (walFile == null) {
                wal = null;
                return;
            }
            if (walFile.getParent() != null) {
                Files.createDirectories(walFile.getParent());
            }
            if (Files.exists(walFile)) {
                replay(walFile);
            }
            wal = Files.newBufferedWriter(walFile, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open write-ahead log " + walFile, e);
        }
    }

    public VersionedValue get(String key) {
        return data.get(key);
    }

    /**
     * Stores the value if it is newer than what is already held.
     *
     * @return true if the value was applied
     */
    public boolean put(String key, VersionedValue value) {
        // Synchronising on the whole engine keeps map state and WAL order consistent.
        synchronized (this) {
            VersionedValue current = data.get(key);
            if (current != null && !value.newerThan(current)) {
                return false;
            }
            append(key, value);
            data.put(key, value);
            return true;
        }
    }

    public int size() {
        return data.size();
    }

    public Map<String, VersionedValue> snapshot() {
        return Map.copyOf(data);
    }

    private void append(String key, VersionedValue v) {
        if (wal == null) return;
        try {
            wal.write(key + " " + v.timestamp() + " " + v.nodeId() + " " + Protocol.encodeValue(v.value()));
            wal.newLine();
            wal.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("Write-ahead log append failed", e);
        }
    }

    private void replay(Path walFile) throws IOException {
        try (BufferedReader in = Files.newBufferedReader(walFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] p = line.split(" ");
                if (p.length != 4) continue; // torn final write after a crash
                VersionedValue v = new VersionedValue(Protocol.decodeValue(p[3]), Long.parseLong(p[1]), p[2]);
                data.merge(p[0], v, VersionedValue::newest);
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (wal != null) wal.close();
    }
}
