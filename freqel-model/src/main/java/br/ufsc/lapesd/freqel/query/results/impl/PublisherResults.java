package br.ufsc.lapesd.freqel.query.results.impl;

import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.query.results.AbstractResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class PublisherResults extends AbstractResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(PublisherResults.class);
    private static final Solution END = MapSolution.build("end", StdLit.fromUnescaped("end"));
    private final @Nonnull BlockingQueue<Solution> queue;
    private final int requestSize;
    private boolean completed;
    private final @Nonnull Object subscriptionState = new Object();
    private int requested = 0;
    private Subscription subscription;
    private @Nullable Solution next;
    private final @Nonnull List<Canceller> cancellerList = new ArrayList<>();

    public PublisherResults(@Nonnull Publisher<Solution> flux, @Nonnull Collection<String> varNames) {
        this(flux, varNames, 2048);
    }

    public PublisherResults(@Nonnull Publisher<Solution> publisher, @Nonnull Collection<String> varNames,
                            int queueSize) {
        this(publisher, varNames, queueSize, Math.max(1, queueSize/4));
    }

    public PublisherResults(@Nonnull Publisher<Solution> publisher, @Nonnull Collection<String> varNames,
                            int queueSize, int requestSize) {
        super(varNames);
        if (requestSize > queueSize)
            throw new IllegalArgumentException("requestSize("+requestSize+") > queueSize("+queueSize+")");
        if (queueSize < 1)
            throw new IllegalArgumentException("queueSize="+queueSize+" should be >= 3");
        if (requestSize < 1)
            throw new IllegalArgumentException("requestSize="+requestSize+" should be >= 1");
        if (publisher instanceof Flux)
            ((Flux<Solution>)publisher).subscribeOn(Schedulers.boundedElastic());
        this.queue = new ArrayBlockingQueue<>(queueSize+2);
        this.requestSize = requestSize;
        this.completed = false;
        subscribe(publisher);
    }

    private void subscribe(@Nonnull Publisher<Solution> publisher) {
        Semaphore subscribed = new Semaphore(0);
        publisher.subscribe(new Subscriber<Solution>() {
            @Override public void onSubscribe(Subscription s) {
                if (subscription != null) {
                    s.cancel();
                } else {
                    subscription = s;
                    tryRequest();
                    subscribed.release();
                }
            }

            @Override public void onNext(Solution solution) {
                queue.add(solution);
                synchronized (subscriptionState) {
                    if (--requested == 0)
                        tryRequest();
                }
            }

            public void onEnd(@Nullable Throwable t) {
                ArrayList<Canceller> copy;
                synchronized (subscriptionState) {
                    copy = new ArrayList<>(cancellerList);
                    completed = true;
                    cancellerList.clear();
                }
                try {
                    if (t != null)
                        logger.error("{}: Error from {}: {}", this, publisher, t, t);
                    else
                        logger.debug("{}: {} completed", this, publisher);
                    queue.add(END);
                } finally {
                    for (Canceller c : copy) c.notifyEnd();
                }
            }

            @Override public void onError(Throwable t) {
                onEnd(t);
            }

            @Override public void onComplete() {
                onEnd(null);
            }
        });
        subscribed.acquireUninterruptibly();
    }

    public class Canceller {
        private final @Nonnull Consumer<Canceller> onCompleteOrError;
        private boolean notified = false;

        public Canceller(@Nonnull Consumer<Canceller> onCompleteOrError) {
            this.onCompleteOrError = onCompleteOrError;
        }

        public @Nonnull PublisherResults getResults() {
            return PublisherResults.this;
        }

        synchronized void notifyEnd() {
            if (notified)
                return;
            notified = true;
            onCompleteOrError.accept(this);
        }

        public synchronized void ifActive(@Nonnull Consumer<Canceller> consumer) {
            if (!notified)
                consumer.accept(this);
        }

        public void cancel() {
            synchronized (subscriptionState) {
                if (!completed) {
                    subscription.cancel();
                    completed = true; //stops new requests
                }
            }
        }
    }

    public @Nonnull Canceller createCanceller(@Nonnull Consumer<Canceller> onCompleteOrError) {
        Canceller canceller = new Canceller(onCompleteOrError);
        synchronized (cancellerList) {
            if (completed)
                canceller.notifyEnd();
            else
                cancellerList.add(canceller);
        }
        return canceller;
    }

    /**
     * Do a {@code subscription.request(requestSize} iff the publisher has not ended and the queue
     * has capacity for all pending requests plus an additional batch of {@code requestSize} items.
     */
    private void tryRequest() {
        synchronized (subscriptionState) {
            if (!completed && requested+requestSize <= queue.remainingCapacity()-2)
                subscription.request(requestSize);
        }
    }

    @Override public boolean isAsync() {
        return true;
    }

    @Override public boolean hasNext() {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    if (next == null) {
                        next = queue.take();
                        tryRequest();
                    }
                    return next != END;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    @Override public boolean hasNext(int millisecondsTimeout) {
        try {
            if (next == null) {
                if ((next = queue.poll(millisecondsTimeout, TimeUnit.MILLISECONDS)) != null)
                    tryRequest();
            }
            return next != null && next != END;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        Solution solution = this.next;
        assert solution != null;
        this.next = null;
        return solution;
    }

    @Override public void close() throws ResultsCloseException {
        subscription.cancel();
    }
}
