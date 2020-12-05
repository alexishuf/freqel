package br.ufsc.lapesd.riefederator.util.parse.iterators;

import org.apache.jena.graph.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Iterator;

public class DelegatingJenaTripleIterator implements JenaTripleIterator {
    private static final Logger logger = LoggerFactory.getLogger(DelegatingJenaTripleIterator.class);
    private final @Nonnull Iterator<Triple> delegate;

    public DelegatingJenaTripleIterator(@Nonnull Iterator<Triple> delegate) {
        this.delegate = delegate;
    }

    @Override public void close() {
        if (delegate instanceof AutoCloseable) {
            try {
                ((AutoCloseable) delegate).close();
            } catch (Exception e) {
                logger.error("Exception closing delegate {}", delegate, e);
            }
        }
    }

    @Override public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override public @Nonnull Triple next() {
        return delegate.next();
    }
}
