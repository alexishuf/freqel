package br.ufsc.lapesd.riefederator.hdt.util;

import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

import java.util.NoSuchElementException;

public class EmptyIteratorTripleString implements IteratorTripleString {
    @Override public void goToStart() {
        /* nothing */
    }

    @Override public long estimatedNumResults() {
        return 0;
    }

    @Override public ResultEstimationType numResultEstimation() {
        return ResultEstimationType.EXACT;
    }

    @Override public boolean hasNext() {
        return false;
    }

    @Override public TripleString next() {
        throw new NoSuchElementException();
    }
}
