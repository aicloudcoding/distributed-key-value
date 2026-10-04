package kv.storage;

/**
 * A value tagged with a version. Conflicts are resolved last-writer-wins: the higher
 * timestamp wins and the writing node's id breaks ties. A {@code null} value is a
 * tombstone recording a delete, so deletes replicate like any other write.
 */
public record VersionedValue(byte[] value, long timestamp, String nodeId) {

    public boolean isTombstone() {
        return value == null;
    }

    public boolean newerThan(VersionedValue other) {
        if (other == null) return true;
        if (timestamp != other.timestamp) return timestamp > other.timestamp;
        return nodeId.compareTo(other.nodeId) > 0;
    }

    public boolean sameVersion(VersionedValue other) {
        return other != null && timestamp == other.timestamp && nodeId.equals(other.nodeId);
    }

    public static VersionedValue newest(VersionedValue a, VersionedValue b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.newerThan(b) ? a : b;
    }
}
