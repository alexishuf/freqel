package br.ufsc.lapesd.freqel.util;

import javax.annotation.Nonnull;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExponentialBackoff implements BackoffStrategy {
    private static final @Nonnull ScheduledExecutorService executor
            = Executors.newScheduledThreadPool(0, new ThreadFactory() {
                private final @Nonnull ThreadGroup group = System.getSecurityManager() == null
                                                 ? Thread.currentThread().getThreadGroup()
                                                 : System.getSecurityManager().getThreadGroup();
                private final @Nonnull AtomicInteger nextThreadId = new AtomicInteger(0);

                @Override public Thread newThread(@Nonnull Runnable r) {
                    String name = "ExponentialBackoff-" + nextThreadId.getAndIncrement();
                    Thread thread = new Thread(group, r, name, 0);
                    if (!thread.isDaemon())
                        thread.setDaemon(true);
                    if (thread.getPriority() != Thread.NORM_PRIORITY)
                        thread.setPriority(Thread.NORM_PRIORITY);
                    return thread;
                }
            });

    private int nextBackoffMs;
    private final int initialBackoffMs;
    private final int maxBackoffCount;
    private int backoffCount;

    public ExponentialBackoff(int nextBackoffMs, int maxBackoffCount) {
        this.nextBackoffMs = this.initialBackoffMs = nextBackoffMs;
        this.maxBackoffCount = maxBackoffCount;
        this.backoffCount = 0;
    }

    public static @Nonnull ExponentialBackoff neverRetry() {
        return new ExponentialBackoff(0, 0);
    }

    public static @Nonnull ExponentialBackoff spamming(int maxRetries) {
        return new ExponentialBackoff(0, maxRetries);
    }

    private int getSleep() {
        if (backoffCount == maxBackoffCount)
            return -1;
        int ms = this.nextBackoffMs;
        nextBackoffMs *= 2;
        ++backoffCount;
        return ms;
    }

    @Override public boolean canRetry() {
        return backoffCount < maxBackoffCount;
    }

    @Override public boolean backOff() {
        int ms = getSleep();
        if (ms > 0) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return ms >= 0;
    }

    @Override public boolean backOff(@Nonnull Runnable onRetry) {
        int ms = getSleep();
        if (ms >= 0)
            executor.schedule(onRetry, ms, TimeUnit.MILLISECONDS);
        return ms >= 0;
    }

    @Override public void reset() {
        nextBackoffMs = initialBackoffMs;
        backoffCount = 0;
    }

    @Override public @Nonnull BackoffStrategy create() {
        return new ExponentialBackoff(initialBackoffMs, maxBackoffCount);
    }
}
