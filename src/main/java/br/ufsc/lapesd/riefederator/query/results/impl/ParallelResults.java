package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsCloseException;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

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
