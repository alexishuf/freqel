package br.ufsc.lapesd.riefederator.util.parse.iterators;

import org.apache.jena.graph.Triple;

import java.util.Iterator;

public interface JenaTripleIterator extends Iterator<Triple>, AutoCloseable {
    @Override void close();
}
