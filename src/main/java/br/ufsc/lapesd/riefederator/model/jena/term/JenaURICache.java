package br.ufsc.lapesd.riefederator.model.jena.term;

import org.jetbrains.annotations.Contract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Integer.MAX_VALUE;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;

@SuppressWarnings("FieldCanBeLocal")
public class JenaURICache {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(JenaURICache.class);
    public static final @Nonnull JenaURICache instance = new JenaURICache();

    private final int CACHE_CAPACITY = 2048;
    private final int CACHE_PURGE_THRESHOLD = 2048 - 2048/4 - 128;
    private final int CACHE_PURGE = 128;

    private static class Entry {
        final @Nonnull JenaURI uri;
        int lastUse;

        public Entry(@Nonnull JenaURI uri, int lastUse) {
            this.uri = uri;
            this.lastUse = lastUse;
        }
    }
    private final @Nonnull ConcurrentHashMap<String, Entry> cache =
            new ConcurrentHashMap<>(CACHE_CAPACITY, 0.75f);
    private AtomicInteger age = new AtomicInteger(0);
    private AtomicInteger size = new AtomicInteger(0);
    private ExecutorService purger = new ThreadPoolExecutor(0, 1,
            30, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1),
            new ThreadPoolExecutor.DiscardPolicy());

    public static @Nonnull JenaURICache getInstance() {
        return instance;
    }

    @Contract("null ->  null; !null -> !null")
    public JenaURI getURI( String uriString) {
        Entry entry = cache.computeIfAbsent(uriString, k -> {
            size.incrementAndGet();
            return new Entry(new JenaURI(createResource(k)), age.incrementAndGet());
        });
        entry.lastUse = age.get();
        if (size.get() == CACHE_PURGE_THRESHOLD)
            purger.execute(this::purge);
        return entry.uri;
    }

    private void purge() {
        try {
            if (size.get() < CACHE_PURGE_THRESHOLD)
                return; // bogus call, too son to purge again
            int youngest = youngestPurge();
            ArrayList<String> victims = new ArrayList<>(CACHE_PURGE);
            for (Map.Entry<String, Entry> e : cache.entrySet()) {
                if (e.getValue().lastUse <= youngest)
                    victims.add(e.getKey());
                if (victims.size() == CACHE_PURGE)
                    break;
            }
            size.addAndGet(-victims.size());
            victims.forEach(cache::remove);
        } catch (Throwable t) {
            logger.error("{}.purge() fail", this==instance ? "JenaURICache.instance":this, t);
        }
    }

    private int youngestPurge() {
        int[] ages = new int[CACHE_CAPACITY];
        int i = 0;
        for (Entry entry : cache.values()) ages[i] = entry.lastUse;
        for (int j = i; j < CACHE_CAPACITY; j++) ages[j] = MAX_VALUE;
        Arrays.sort(ages);
        return ages[CACHE_PURGE];
    }
}
