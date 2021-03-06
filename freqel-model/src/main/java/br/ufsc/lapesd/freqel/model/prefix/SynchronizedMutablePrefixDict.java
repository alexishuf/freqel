package br.ufsc.lapesd.freqel.model.prefix;

import br.ufsc.lapesd.freqel.model.term.URI;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.function.BiConsumer;

@ThreadSafe
public class SynchronizedMutablePrefixDict implements MutablePrefixDict {
    private static final Logger logger = LoggerFactory.getLogger(SynchronizedMutablePrefixDict.class);
    private final @Nonnull MutablePrefixDict delegate;

    public SynchronizedMutablePrefixDict(@Nonnull MutablePrefixDict delegate) {
        this.delegate = delegate;
    }

    @Override
    public synchronized @Nullable String put(@Nonnull String prefix, @Nonnull String uri) {
        return delegate.put(prefix, uri);
    }

    @Override
    public synchronized @Nullable String remove(@Nonnull String prefix) {
        return delegate.remove(prefix);
    }

    /**
     * <b>Warning:</b> Calling this directly, iteration will be in race with updates to
     * the {@link MutablePrefixDict}. <b>Use <code>forEach()</code> instead</b>.
     *
     * @return non-thread-safe {@link Iterable} over the prefix-uri entries.
     */
    @Override
    @GuardedBy("this")
    public @Nonnull Iterable<Map.Entry<String, String>> entries() {
        if (!Thread.holdsLock(this)) {
            logger.warn("Thread {} called entries() without holding monitor lock. Will dump a " +
                    "stack trace, but continue.", Thread.currentThread().getName(),
                    new IllegalStateException("entries() without owning monitor"));
        }
        return delegate.entries();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public synchronized void forEach(BiConsumer<String, String> consumer) {
        delegate.forEach(consumer);
    }

    @Override
    public synchronized @Nonnull Shortened shorten(@Nonnull String uri) {
        return delegate.shorten(uri);
    }

    @Override
    @Contract(value = "_, !null -> !null", pure = true)
    public synchronized String expandPrefix(@Nonnull String shortPrefix, String fallback) {
        return delegate.expandPrefix(shortPrefix, fallback);
    }

    @Override
    public synchronized @Nonnull Shortened shorten(@Nonnull URI uri) {
        return delegate.shorten(uri);
    }

    @Override
    @Contract("_, !null -> !null")
    public synchronized String shortenPrefix(@Nonnull String longPrefix, String fallback) {
        return delegate.shortenPrefix(longPrefix, fallback);
    }

    @Override
    public synchronized @Nonnull String shortenPrefix(@Nonnull String longPrefix) {
        return delegate.shortenPrefix(longPrefix);
    }

    @Override
    @Contract("_, !null -> !null")
    public synchronized String expand(@Nonnull String shortened, String fallback) {
        return delegate.expand(shortened, fallback);
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    @Override
    public synchronized boolean sameEntries(@Nullable PrefixDict other) {
        if (other == null) return false;
        synchronized (other) {
            return delegate.sameEntries(other);
        }
    }
}
