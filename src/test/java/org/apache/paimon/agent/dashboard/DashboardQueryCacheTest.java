package org.apache.paimon.agent.dashboard;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DashboardQueryCacheTest {

    @Test
    void expiresAndEvictsByEntryCountAndWeight() throws Exception {
        AtomicLong now = new AtomicLong();
        DashboardQueryCache<String, String> cache =
                new DashboardQueryCache<>(
                        Duration.ofSeconds(10), 2, 6L, String::length, now::get);
        AtomicInteger loads = new AtomicInteger();

        assertThat(cache.get("a", () -> "aa-" + loads.incrementAndGet())).isEqualTo("aa-1");
        assertThat(cache.get("a", () -> "unused")).isEqualTo("aa-1");
        assertThat(loads).hasValue(1);

        cache.get("b", () -> "bb");
        assertThat(cache.size()).isEqualTo(2);
        assertThat(cache.weight()).isEqualTo(6L);
        cache.get("c", () -> "cc");
        assertThat(cache.size()).isEqualTo(2);
        assertThat(cache.get("a", () -> "aa-" + loads.incrementAndGet())).isEqualTo("aa-2");

        now.set(Duration.ofSeconds(11).toNanos());
        assertThat(cache.get("a", () -> "aa-" + loads.incrementAndGet())).isEqualTo("aa-3");
        cache.clear();
        assertThat(cache.size()).isZero();
        assertThat(cache.weight()).isZero();
    }

    @Test
    void coalescesConcurrentLoadsAndDoesNotCacheFailures() throws Exception {
        DashboardQueryCache<String, String> cache =
                new DashboardQueryCache<>(Duration.ofSeconds(10), 4, 100L, value -> 1L);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger loads = new AtomicInteger();
        try {
            Future<String> first =
                    executor.submit(
                            () ->
                                    cache.get(
                                            "same",
                                            () -> {
                                                loads.incrementAndGet();
                                                entered.countDown();
                                                release.await();
                                                return "value";
                                            }));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            Future<String> second =
                    executor.submit(
                            () ->
                                    cache.get(
                                            "same",
                                            () -> {
                                                loads.incrementAndGet();
                                                return "other";
                                            }));
            release.countDown();

            assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo("value");
            assertThat(second.get(5, TimeUnit.SECONDS)).isEqualTo("value");
            assertThat(loads).hasValue(1);

            assertThatThrownBy(
                            () ->
                                    cache.get(
                                            "failure",
                                            () -> {
                                                throw new IllegalStateException("boom");
                                            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("boom");
            assertThat(cache.get("failure", () -> "recovered")).isEqualTo("recovered");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void clearDoesNotAllowAnInFlightLoadToRepopulateTheCache() throws Exception {
        DashboardQueryCache<String, String> cache =
                new DashboardQueryCache<>(Duration.ofSeconds(10), 4, 100L, value -> 1L);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<String> result =
                    executor.submit(
                            () ->
                                    cache.get(
                                            "key",
                                            () -> {
                                                entered.countDown();
                                                release.await();
                                                return "old";
                                            }));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            cache.clear();
            Future<String> refreshed =
                    executor.submit(() -> cache.get("key", () -> "new"));
            assertThat(refreshed.get(5, TimeUnit.SECONDS)).isEqualTo("new");
            release.countDown();
            assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo("old");
            assertThat(cache.size()).isEqualTo(1);
            assertThat(cache.get("key", () -> "unused")).isEqualTo("new");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void appliesAnIndependentSecondaryWeightLimit() throws Exception {
        DashboardQueryCache<String, String> cache =
                new DashboardQueryCache<>(
                        Duration.ofSeconds(10),
                        10,
                        10L,
                        ignored -> 1L,
                        5L,
                        String::length);

        cache.get("a", () -> "aaa");
        cache.get("b", () -> "bbb");
        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.weight()).isEqualTo(1L);
        assertThat(cache.secondaryWeight()).isEqualTo(3L);

        cache.get("too-large", () -> "123456");
        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.secondaryWeight()).isEqualTo(3L);
    }
}
