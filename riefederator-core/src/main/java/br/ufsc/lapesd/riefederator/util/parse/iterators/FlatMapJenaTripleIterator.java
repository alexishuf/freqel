package br.ufsc.lapesd.riefederator.util.parse.iterators;

import org.apache.jena.graph.Triple;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

public class FlatMapJenaTripleIterator implements JenaTripleIterator {
    private boolean closed = false;
    private final @Nonnull Iterator<Supplier<JenaTripleIterator>> supplierIt;
    private JenaTripleIterator current;

    public FlatMapJenaTripleIterator(@Nonnull Iterator<Supplier<JenaTripleIterator>> it) {
        this.supplierIt = it;
    }

    @Override public void close() {
        closed = true;
        if (current != null) {
            current.close();
            current = null;
        }
    }

    @Override public boolean hasNext() {
        if (closed) return false;
        while ((current == null || !current.hasNext()) && supplierIt.hasNext()) {
            if (current != null) current.close();
            current = supplierIt.next().get();
        }
        if (current != null) {
            if (current.hasNext())
                return true;
            close();
        }
        return false;
    }

    @Override public Triple next() {
        if (!hasNext()) throw new NoSuchElementException();
        return current.next();
    }
}
