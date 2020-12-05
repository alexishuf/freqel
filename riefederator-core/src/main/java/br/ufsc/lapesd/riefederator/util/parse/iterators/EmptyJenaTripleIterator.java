package br.ufsc.lapesd.riefederator.util.parse.iterators;

import org.apache.jena.graph.Triple;

import java.util.NoSuchElementException;

public class EmptyJenaTripleIterator implements JenaTripleIterator {
    @Override public void close() { }

    @Override public boolean hasNext() {
        return false;
    }

    @Override public Triple next() {
        throw new NoSuchElementException();
    }
}
