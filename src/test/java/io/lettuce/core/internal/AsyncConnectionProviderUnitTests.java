/*
 * Copyright 2011-Present, Redis Ltd. and Contributors
 * All rights reserved.
 *
 * Licensed under the MIT License.
 */
package io.lettuce.core.internal;

import static io.lettuce.TestTags.UNIT_TEST;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.lettuce.test.ReflectionTestUtils;

/**
 * Unit tests for {@link AsyncConnectionProvider} cache ownership.
 * <p>
 * A failed connection attempt and {@code close(key)} must drop only the synchronizer they observed. A newer connection cached
 * under the same key has to stay reachable.
 */
@Tag(UNIT_TEST)
@Timeout(10)
class AsyncConnectionProviderUnitTests {

    private static final String KEY = "node-1";

    @Test
    void failedAcquisitionShouldNotEvictReplacement() {

        AttemptFactory factory = new AttemptFactory();
        AsyncConnectionProvider<String, TrackingConnection, CompletableFuture<TrackingConnection>> provider = new AsyncConnectionProvider<>(
                factory);

        provider.getConnection(KEY);
        assertThat(factory.attempts()).hasSize(1);

        TrackingConnection replacement = new TrackingConnection();
        provider.register(KEY, replacement);

        factory.attempts().get(0).completeExceptionally(new IllegalStateException("stale attempt"));

        assertThat(provider.getConnection(KEY)).isCompletedWithValue(replacement);
        assertThat(factory.attempts()).hasSize(1);
        assertThat(provider.getConnectionCount()).isOne();
        assertThat(replacement.closeCount()).isZero();
    }

    @Test
    void failedAcquisitionShouldNotEvictConnectionAcquiredAfterClose() {

        AttemptFactory factory = new AttemptFactory();
        AsyncConnectionProvider<String, TrackingConnection, CompletableFuture<TrackingConnection>> provider = new AsyncConnectionProvider<>(
                factory);

        provider.getConnection(KEY);
        provider.close(KEY);
        CompletableFuture<TrackingConnection> reacquired = provider.getConnection(KEY);

        TrackingConnection replacement = new TrackingConnection();
        factory.attempts().get(1).complete(replacement);
        assertThat(reacquired).isCompletedWithValue(replacement);

        factory.attempts().get(0).completeExceptionally(new IllegalStateException("stale attempt"));

        assertThat(provider.getConnectionCount()).isOne();
        assertThat(provider.getConnection(KEY)).isCompletedWithValue(replacement);
        assertThat(factory.attempts()).hasSize(2);
        assertThat(replacement.closeCount()).isZero();
    }

    @Test
    void closeShouldNotEvictReplacementInstalledAfterLookup() throws Exception {

        AtomicInteger created = new AtomicInteger();
        AsyncConnectionProvider<String, TrackingConnection, CompletableFuture<TrackingConnection>> provider = new AsyncConnectionProvider<>(
                key -> {
                    created.incrementAndGet();
                    return CompletableFuture.completedFuture(new TrackingConnection());
                });

        LookupGate<Object, Object> gate = new LookupGate<>();
        ReflectionTestUtils.setField(provider, "connections", gate);

        TrackingConnection original = new TrackingConnection();
        provider.register(KEY, original);
        assertThat(provider.getConnectionCount()).isOne();
        assertThat(gate).hasSize(1);

        // Pause inside Map.get so a replacement can be installed before close(key) removes the entry.
        gate.armNextLookup.set(true);
        AtomicReference<Throwable> closerFailure = new AtomicReference<>();
        Thread closer = new Thread(() -> {
            try {
                provider.close(KEY);
            } catch (Throwable throwable) {
                closerFailure.set(throwable);
            }
        }, "close-key");
        closer.setDaemon(true);
        closer.start();

        TrackingConnection replacement = new TrackingConnection();
        try {
            assertThat(gate.lookupEntered.await(5, TimeUnit.SECONDS)).isTrue();
            provider.register(KEY, replacement);
            assertThat(gate).hasSize(1);
        } finally {
            gate.releaseLookup.countDown();
        }

        closer.join(5000);
        assertThat(closer.isAlive()).isFalse();
        assertThat(closerFailure.get()).isNull();

        assertThat(provider.getConnection(KEY)).isCompletedWithValue(replacement);
        assertThat(created).hasValue(0);
        assertThat(original.closeCount()).isOne();
        assertThat(replacement.closeCount()).isZero();
        assertThat(provider.getConnectionCount()).isOne();
    }

    @Test
    void failedAcquisitionShouldEvictItsOwnSynchronizer() {

        AttemptFactory factory = new AttemptFactory();
        AsyncConnectionProvider<String, TrackingConnection, CompletableFuture<TrackingConnection>> provider = new AsyncConnectionProvider<>(
                factory);

        provider.getConnection(KEY);
        factory.attempts().get(0).completeExceptionally(new IllegalStateException("connect failed"));

        assertThat(provider.getConnectionCount()).isZero();

        provider.getConnection(KEY);
        assertThat(factory.attempts()).hasSize(2);
    }

    @Test
    void closeShouldRemoveAndCloseTheCurrentConnection() {

        AttemptFactory factory = new AttemptFactory();
        AsyncConnectionProvider<String, TrackingConnection, CompletableFuture<TrackingConnection>> provider = new AsyncConnectionProvider<>(
                factory);

        TrackingConnection current = new TrackingConnection();
        provider.register(KEY, current);
        assertThat(provider.getConnectionCount()).isOne();

        provider.close(KEY);

        assertThat(current.closeCount()).isOne();
        assertThat(provider.getConnectionCount()).isZero();
        assertThat(factory.attempts()).isEmpty();

        provider.getConnection(KEY);
        assertThat(factory.attempts()).hasSize(1);
    }

    private static final class AttemptFactory implements Function<String, CompletableFuture<TrackingConnection>> {

        private final List<CompletableFuture<TrackingConnection>> attempts = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<TrackingConnection> apply(String key) {
            CompletableFuture<TrackingConnection> attempt = new CompletableFuture<>();
            attempts.add(attempt);
            return attempt;
        }

        List<CompletableFuture<TrackingConnection>> attempts() {
            return attempts;
        }

    }

    private static final class TrackingConnection implements AsyncCloseable {

        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public CompletableFuture<Void> closeAsync() {
            closeCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }

        int closeCount() {
            return closeCount.get();
        }

    }

    /**
     * Test-only cache that can pause one {@link #get(Object)} between reading the current value and returning it.
     */
    private static final class LookupGate<K, V> extends ConcurrentHashMap<K, V> {

        private final AtomicBoolean armNextLookup = new AtomicBoolean();

        private final CountDownLatch lookupEntered = new CountDownLatch(1);

        private final CountDownLatch releaseLookup = new CountDownLatch(1);

        @Override
        public V get(Object key) {
            V value = super.get(key);
            if (armNextLookup.compareAndSet(true, false)) {
                lookupEntered.countDown();
                try {
                    if (!releaseLookup.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("timed out waiting to resume connection lookup");
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted while pausing connection lookup", ex);
                }
            }
            return value;
        }

    }

}
