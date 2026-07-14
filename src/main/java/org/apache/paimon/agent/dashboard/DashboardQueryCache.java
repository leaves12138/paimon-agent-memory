package org.apache.paimon.agent.dashboard;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

/** Small TTL/LRU cache which also coalesces concurrent loads for the same dashboard query. */
final class DashboardQueryCache<K, V> {

    private final long ttlNanos;
    private final int maxEntries;
    private final long maxWeight;
    private final ToLongFunction<V> weigher;
    private final long maxSecondaryWeight;
    private final ToLongFunction<V> secondaryWeigher;
    private final LongSupplier nanoTime;
    private final LinkedHashMap<K, Entry<V>> entries;
    private final Map<K, CompletableFuture<V>> loads;

    private long weight;
    private long secondaryWeight;
    private long epoch;

    DashboardQueryCache(
            Duration ttl, int maxEntries, long maxWeight, ToLongFunction<V> weigher) {
        this(
                ttl,
                maxEntries,
                maxWeight,
                weigher,
                Long.MAX_VALUE,
                ignored -> 0L,
                System::nanoTime);
    }

    DashboardQueryCache(
            Duration ttl,
            int maxEntries,
            long maxWeight,
            ToLongFunction<V> weigher,
            long maxSecondaryWeight,
            ToLongFunction<V> secondaryWeigher) {
        this(
                ttl,
                maxEntries,
                maxWeight,
                weigher,
                maxSecondaryWeight,
                secondaryWeigher,
                System::nanoTime);
    }

    DashboardQueryCache(
            Duration ttl,
            int maxEntries,
            long maxWeight,
            ToLongFunction<V> weigher,
            LongSupplier nanoTime) {
        this(
                ttl,
                maxEntries,
                maxWeight,
                weigher,
                Long.MAX_VALUE,
                ignored -> 0L,
                nanoTime);
    }

    private DashboardQueryCache(
            Duration ttl,
            int maxEntries,
            long maxWeight,
            ToLongFunction<V> weigher,
            long maxSecondaryWeight,
            ToLongFunction<V> secondaryWeigher,
            LongSupplier nanoTime) {
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be greater than zero");
        }
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be greater than zero");
        }
        if (maxWeight <= 0) {
            throw new IllegalArgumentException("maxWeight must be greater than zero");
        }
        if (maxSecondaryWeight <= 0) {
            throw new IllegalArgumentException("maxSecondaryWeight must be greater than zero");
        }
        this.ttlNanos = ttl.toNanos();
        this.maxEntries = maxEntries;
        this.maxWeight = maxWeight;
        this.weigher = Objects.requireNonNull(weigher, "weigher");
        this.maxSecondaryWeight = maxSecondaryWeight;
        this.secondaryWeigher =
                Objects.requireNonNull(secondaryWeigher, "secondaryWeigher");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.entries = new LinkedHashMap<>(16, 0.75f, true);
        this.loads = new LinkedHashMap<>();
    }

    V get(K key, Loader<V> loader) throws Exception {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(loader, "loader");
        CompletableFuture<V> load;
        boolean owner = false;
        long loadEpoch = -1L;
        synchronized (this) {
            long now = nanoTime.getAsLong();
            Entry<V> cached = entries.get(key);
            if (cached != null) {
                if (now - cached.createdAtNanos < ttlNanos) {
                    return cached.value;
                }
                remove(key, cached);
            }
            load = loads.get(key);
            if (load == null) {
                load = new CompletableFuture<>();
                loads.put(key, load);
                owner = true;
                loadEpoch = epoch;
            }
        }

        if (!owner) {
            return await(load);
        }

        try {
            V value = Objects.requireNonNull(loader.load(), "loader result");
            synchronized (this) {
                loads.remove(key, load);
                if (loadEpoch == epoch) {
                    put(key, value, nanoTime.getAsLong());
                }
            }
            load.complete(value);
            return value;
        } catch (Throwable failure) {
            synchronized (this) {
                loads.remove(key, load);
            }
            load.completeExceptionally(failure);
            rethrow(failure);
            throw new AssertionError("unreachable");
        }
    }

    synchronized void clear() {
        epoch++;
        entries.clear();
        // A caller explicitly asking for a refresh must not join a load which started before
        // this invalidation. The old owner still completes its own future, but its epoch prevents
        // it from repopulating the cache and remove(key, value) cannot remove the replacement.
        loads.clear();
        weight = 0L;
        secondaryWeight = 0L;
    }

    synchronized int size() {
        return entries.size();
    }

    synchronized long weight() {
        return weight;
    }

    synchronized long secondaryWeight() {
        return secondaryWeight;
    }

    private void put(K key, V value, long now) {
        long valueWeight = Math.max(1L, weigher.applyAsLong(value));
        long valueSecondaryWeight = Math.max(0L, secondaryWeigher.applyAsLong(value));
        if (valueWeight > maxWeight || valueSecondaryWeight > maxSecondaryWeight) {
            return;
        }
        Entry<V> previous = entries.remove(key);
        if (previous != null) {
            weight -= previous.weight;
            secondaryWeight -= previous.secondaryWeight;
        }
        entries.put(key, new Entry<>(value, now, valueWeight, valueSecondaryWeight));
        weight += valueWeight;
        secondaryWeight += valueSecondaryWeight;
        Iterator<Map.Entry<K, Entry<V>>> iterator = entries.entrySet().iterator();
        while ((entries.size() > maxEntries
                        || weight > maxWeight
                        || secondaryWeight > maxSecondaryWeight)
                && iterator.hasNext()) {
            Entry<V> eldest = iterator.next().getValue();
            weight -= eldest.weight;
            secondaryWeight -= eldest.secondaryWeight;
            iterator.remove();
        }
    }

    private void remove(K key, Entry<V> entry) {
        entries.remove(key);
        weight -= entry.weight;
        secondaryWeight -= entry.secondaryWeight;
    }

    private static <V> V await(CompletableFuture<V> future) throws Exception {
        try {
            return future.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (ExecutionException failed) {
            rethrow(failed.getCause());
            throw new AssertionError("unreachable");
        }
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    @FunctionalInterface
    interface Loader<V> {
        V load() throws Exception;
    }

    private static final class Entry<V> {
        private final V value;
        private final long createdAtNanos;
        private final long weight;
        private final long secondaryWeight;

        private Entry(
                V value, long createdAtNanos, long weight, long secondaryWeight) {
            this.value = value;
            this.createdAtNanos = createdAtNanos;
            this.weight = weight;
            this.secondaryWeight = secondaryWeight;
        }
    }
}
