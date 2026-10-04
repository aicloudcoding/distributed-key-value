package kv.storage;

/**
 * Hybrid logical clock: tracks wall-clock time but never goes backwards and always moves
 * past any timestamp it has observed from other nodes. This keeps causally later writes
 * ordered after earlier ones even when node clocks drift a little.
 */
public final class HybridClock {

    private long last;

    public synchronized long next() {
        last = Math.max(System.currentTimeMillis() * 1000, last + 1);
        return last;
    }

    public synchronized void observe(long timestamp) {
        last = Math.max(last, timestamp);
    }
}
