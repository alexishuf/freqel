package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.error.ResultsCloseException;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.query.Cardinality.Reliability.*;

public class ParallelResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(ParallelResults.class);
    private ImmutableSet<String> varNames;

    private final @Nonnull List<Results> resultsList;
    private Results next = null;

    public ParallelResults(@Nonnull List<Results> resultsList) {
        this.resultsList = resultsList;
        ImmutableSet.Builder<String> b = ImmutableSet.builder();
        for (Results child : resultsList) b.addAll(child.getVarNames());
        varNames = b.build();
    }

    @Override
    public int getReadyCount() {
        int sum = 0;
        for (Results child : resultsList) sum += child.getReadyCount();
        return sum;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext()) throw new NoSuchElementException();
        Solution solution = this.next.next();
        next = null;
        return solution;
    }

    @Override
    public @Nonnull Cardinality getCardinality() {
        if (resultsList.size() == 1)
            return resultsList.get(0).getCardinality();
        boolean unsupported = false;
        int guess = 0, upperBound = 0, lowerBound = 0, exact = 0;
        for (Results child : resultsList) {
            Cardinality card = child.getCardinality();
            switch (card.getReliability()) {
                case UNSUPPORTED: unsupported = true; break;
                case   NON_EMPTY: ++lowerBound      ; break;
                case       GUESS: guess      += card.getValue(0); break;
                case LOWER_BOUND: lowerBound += card.getValue(0); break;
                case UPPER_BOUND: upperBound += card.getValue(0); break;
                case       EXACT: exact      += card.getValue(0); break;
                default: break;
            }
        }
        if (guess > 0)
            return new Cardinality(GUESS, guess+lowerBound+upperBound+exact);
        if (lowerBound > 0)
            return new Cardinality(LOWER_BOUND, lowerBound+upperBound+exact);
        if (upperBound > 0)
            return new Cardinality(unsupported ? LOWER_BOUND : UPPER_BOUND, upperBound+exact);
        if (exact > 0)
            return new Cardinality(unsupported ? LOWER_BOUND : EXACT, exact);
        return hasNext() ? Cardinality.NON_EMPTY : Cardinality.EMPTY;
    }

    @Override
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }

    @Override
    public void close() throws ResultsCloseException {
        List<ResultsCloseException> exceptions = new ArrayList<>();
        for (Results child : resultsList) {
            try {
                child.close();
            } catch (ResultsCloseException e) {
                logger.error("Problem closing child {}", child, e);
                exceptions.add(e);
            }
        }
        if (!exceptions.isEmpty())
            throw exceptions.get(0);
    }

    @Override
    public boolean hasNext() {
        if (next != null && next.hasNext()) return true;
        for (Results child : resultsList) {
            if (child.hasNext()) {
                next = child;
                break;
            }
        }
        assert next == null || next.hasNext();
        return next != null;
    }
}
