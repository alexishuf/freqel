package br.ufsc.lapesd.freqel.webapis.requests.impl;

import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.annotations.GlobalContextAnnotation;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.webapis.requests.paging.PagingStrategy;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class QueryRequestCache {
    private static final Logger logger = LoggerFactory.getLogger(QueryRequestCache.class);
    public static final @Nonnull String KEY = QueryRequestCache.class.getSimpleName();

    private final @Nonnull Map<String, Entry> cache = new HashMap<>();
    private final @Nonnull Set<String> fetching = new HashSet<>();

    public static class Entry {
        public final @Nonnull CQEndpoint ep;
        public final @Nonnull PagingStrategy.Pager.State pagerState;

        public Entry(@Nonnull CQEndpoint ep, @Nonnull PagingStrategy.Pager.State pagerState) {
            this.ep = ep;
            this.pagerState = pagerState;
        }
    }

    public static @Nonnull QueryRequestCache getCache(@Nonnull CQuery query,
                                                      @Nonnull TPEndpoint endpoint) {
        GlobalContextAnnotation ann = query.getQueryAnnotation(GlobalContextAnnotation.class);
        if (ann == null) {
            if (query instanceof MutableCQuery)
                ((MutableCQuery)query).annotate(ann = new GlobalContextAnnotation());
            else
                return new QueryRequestCache();
        }
        ImmutablePair<String, TPEndpoint> key = ImmutablePair.of(KEY, endpoint);
        return ann.computeIfAbsent(key, k -> new QueryRequestCache());
    }

    public @Nonnull Entry get(@Nonnull String uri, Supplier<Entry> invoker) {
        Entry cached;
        synchronized (this) {
            cached = cache.get(uri);
            if (cached != null)
                return cached;
            if (!fetching.add(uri)) {
                boolean interrupted = false;
                while (fetching.contains(uri)) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                        logger.warn("Suppressed interrupt while waiting for fetch of {}", uri);
                    }
                }
                cached = cache.get(uri);
                assert cached != null;
                if (interrupted)
                    Thread.currentThread().interrupt();
            }
        }
        if (cached == null) {
            Entry parsed = null;
            try {
                parsed = invoker.get();
                assert parsed != null;
            } finally {
                synchronized (this) {
                    if (parsed != null) //no exception thrown
                        cache.put(uri, parsed);
                    fetching.remove(uri); //with CQEndpoint or exception, we are done fetching
                    notifyAll();
                    cached = parsed;
                }
            }
        }
        return cached;
    }

}
