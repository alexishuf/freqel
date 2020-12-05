package br.ufsc.lapesd.riefederator.util.parse;

import br.ufsc.lapesd.riefederator.util.parse.impl.EstimatorsLib;
import br.ufsc.lapesd.riefederator.util.parse.impl.JenaTripleIteratorFactoriesLib;
import br.ufsc.lapesd.riefederator.util.parse.impl.StreamersLib;
import br.ufsc.lapesd.riefederator.util.parse.iterators.FlatMapJenaTripleIterator;
import br.ufsc.lapesd.riefederator.util.parse.iterators.JenaTripleIterator;
import br.ufsc.lapesd.riefederator.util.parse.iterators.StreamConsumerJenaTripleIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class RDFIterationDispatcher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RDFIterationDispatcher.class);
    private static final RDFIterationDispatcher INSTANCE;

    static {
        INSTANCE = new RDFIterationDispatcher();
        StreamersLib.registerAll(INSTANCE);
        JenaTripleIteratorFactoriesLib.registerAll(INSTANCE);
        EstimatorsLib.registerAll(INSTANCE);
    }

    private final @Nonnull ThreadPoolExecutor executor;
    private final @Nonnull Map<Class<?>, TripleCountEstimator> estimators = new HashMap<>();
    private final @Nonnull Map<Class<?>, JenaTripleIteratorFactory> itFactories = new HashMap<>();
    private final @Nonnull Map<Class<?>, RDFStreamer> streamers = new LinkedHashMap<>();

    public RDFIterationDispatcher() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        SecurityManager secMgr = System.getSecurityManager();
        executor = new ThreadPoolExecutor(0, availableProcessors * 8,
                                          5, TimeUnit.SECONDS,
                                           new LinkedBlockingQueue<>(), new ThreadFactory() {
            private AtomicInteger threads = new AtomicInteger(0);
            ThreadGroup group = secMgr != null ? secMgr.getThreadGroup()
                                               : Thread.currentThread().getThreadGroup();

            @Override public Thread newThread(@Nonnull Runnable r) {
                String name = "RDFIterationDispatcher-" + threads.incrementAndGet();
                Thread thread = new Thread(group, r, name, 0);
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    @Override public void close() {
        if (this == INSTANCE)
            logger.warn("Calling close() on singleton! Not a good idea");
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                logger.warn("close({}) will not wait non-terminating executor", this);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); //restore interrupt flag
        }
    }

    public static @Nonnull RDFIterationDispatcher get() {
        return INSTANCE;
    }

    public void register(@Nonnull Class<?> cls, @Nonnull TripleCountEstimator estimator) {
        estimators.put(cls, estimator);
        estimator.attachTo(this);
    }

    public void register(@Nonnull Class<?> cls, @Nonnull JenaTripleIteratorFactory factory) {
        itFactories.put(cls, factory);
        factory.attachTo(this);
    }

    public void register(@Nonnull Class<?> cls, @Nonnull RDFStreamer streamer) {
        streamers.put(cls, streamer);
        streamer.attachTo(this);
    }

    /**
     * Estimate the number of triples in the source as quickly as possible.
     *
     * @param source the RDF source
     * @return an estimate of the number of triples or -1.
     */
    public long estimate(@Nonnull Object source) {
        if (source instanceof Collection)
            return estimateAll((Collection<?>) source).getEstimate();
        for (Class<?> cls : getClassQueue(source)) {
            TripleCountEstimator estimator = estimators.get(cls);
            if (estimator != null) {
                return estimator.estimate(source);
            }
        }
        return -1;
    }

    /**
     * Estimates the number of non-unique triples in all sources.
     *
     * @param sources Collection of RDF sources
     * @return A {@link TriplesEstimate}
     */
    public @Nonnull TriplesEstimate estimateAll(@Nonnull Collection<?> sources) {
        long total = 0;
        int ignored = 0;
        for (Object source : sources) {
            long estimate = estimate(source);
            if (estimate < 0) ++ignored;
            else              total += estimate;
        }
        if (ignored == sources.size())
            total = -1;
        return new TriplesEstimate(total, ignored, sources.size());
    }

    public @Nonnull JenaTripleIterator
    parse(@Nonnull Object source) throws SourceIterationException {
        if (source instanceof Collection)
            return parseAll((Collection<?>) source);
        for (Class<?> cls : getClassQueue(source)) {
            JenaTripleIteratorFactory factory = itFactories.get(cls);
            if (factory != null && factory.canCreate(source))
                return factory.create(source);
            RDFStreamer streamer = streamers.get(cls);
            if (streamer != null && streamer.canStream(source)) {
                StreamConsumerJenaTripleIterator it = new StreamConsumerJenaTripleIterator();
                executor.execute(() -> streamer.stream(source, it.getStreamRDF()));
                return it;
            }
        }
        throw new SourceIterationException(source, "No iterator factory nor streamer for class "+
                                           source.getClass());
    }

    public @Nonnull JenaTripleIterator parseAll(@Nonnull Collection<?> sources) {
        return new FlatMapJenaTripleIterator(
                sources.stream().map(s -> (Supplier<JenaTripleIterator>)(() -> parse(s)))
                                .iterator()
        );
    }

    private @Nonnull Iterable<Class<?>> getClassQueue(@Nonnull Object src) {
        ArrayDeque<Class<?>> queue = new ArrayDeque<>();
        for (Class<?> cls = src.getClass(); !cls.equals(Object.class); cls = cls.getSuperclass())
            queue.add(cls);
        return new Iterable<Class<?>>() {
            @Override public @Nonnull Iterator<Class<?>> iterator() {
                return new Iterator<Class<?>>() {
                    @Override public boolean hasNext() {
                        return !queue.isEmpty();
                    }

                    @Override public Class<?> next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        Class<?> next = queue.remove();
                        for (Class<?> anInterface : next.getInterfaces())
                            queue.add(anInterface);
                        return next;
                    }
                };
            }
        };
    }


}
