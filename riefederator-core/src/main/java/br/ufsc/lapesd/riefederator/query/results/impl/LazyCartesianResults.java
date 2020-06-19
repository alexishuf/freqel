package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.*;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class LazyCartesianResults extends AbstractResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(LazyCartesianResults.class);
    private final @Nonnull ResultsList<BufferedResults> inputs;
    private final @Nonnull ArraySolution.ValueFactory factory;
    private final @Nonnull Solution[] solutions;
    private @Nullable Solution current;
    private boolean initialized = false, exhausted = false;

    public LazyCartesianResults(@Nonnull Collection<Results> ins,
                                @Nonnull Set<String> varNames) {
        this(ins, varNames, Function.identity());

    }

    public LazyCartesianResults(@Nonnull Collection<Results> ins, @Nonnull Set<String> varNames,
                                @Nonnull Function<Results, Results> parallelFactory) {
        super(varNames);
        int size = ins.size();
        this.inputs = new ResultsList<>(size);
        this.solutions = new Solution[size];
        for (Results in : ins) {
            Results dr = parallelFactory.apply(HashDistinctResults.applyIfNotDistinct(in));
            BufferedResults br = ListBufferedResults.applyIfNotBuffered(dr);
            this.inputs.add(br);
        }
        factory = ArraySolution.forVars(getVarNames());

        // check varNames and non-joinable components
        if (LazyCartesianResults.class.desiredAssertionStatus()) {
            IndexedSet<String> all = IndexedSet.fromDistinct(
                    ins.stream().flatMap(i -> i.getVarNames().stream()).collect(toSet()));
            assert all.containsAll(varNames) : "Some result variables not found in any input";
            List<IndexedSubset<String>> varSets =
                    ins.stream().map(r -> all.subset(r.getVarNames())).collect(toList());
            assert varSets.size() == size;
            for (int i = 0; i < size; i++) {
                IndexedSubset<String> left = varSets.get(i);
                for (int j = i+1; j < size; j++) {
                    assert left.createIntersection(varSets.get(j)).isEmpty()
                            : i+"-th and "+j+"-th input Results share variables, should be joining";
                }
            }
        }
    }

    @Override
    public int getReadyCount() {
        int ready = 1;
        for (Results in : inputs) {
            int next = ready * in.getReadyCount();
            if (next < ready) {
                logger.error("LazyCartesianResults {} has so many results ready their number overflows", this);
                return Integer.MAX_VALUE; //overflow
            }
        }
        return ready;
    }

    @Override
    public boolean isDistinct() {
        return true; //since components are distinct and share no variables
    }

    private boolean advance() {
        if (exhausted)
            return false;
        if (!initialized)
            return initSolutions();
        for (int i = inputs.size()-1; i >= 0; i--) {
            BufferedResults r = inputs.get(i);
            assert r != null;
            if (r.hasNext()) {
                solutions[i] = r.next();
                return true; // done, can assemble a new solution
            } else {
                r.reset(true);
                solutions[i] = r.next();
                // continue iteration
            }
        }
        exhausted = false; // no new solutions
        return false;
    }

    private boolean initSolutions() {
        assert !initialized;
        initialized = true;
        int size = inputs.size();
        assert solutions.length == size;
        for (int i = 0; i < size; i++) {
            BufferedResults br = inputs.get(i);
            if (br.hasNext()) {
                solutions[i] = br.next();
            } else { // no solutions possible, ever
                exhausted = true;
                return false; // avoid hasNext() calls
            }
        }
        return true; // can assemble a solution
    }

    @Override
    public boolean hasNext() {
        if (current == null) {
            if (advance())
                current = factory.fromSolutions(solutions);
        }
        return current != null;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        assert initialized && !exhausted;
        Solution solution = this.current;
        assert solution != null;
        this.current = null;
        return solution;
    }

    @Override
    public void close() throws ResultsCloseException {
        inputs.close();
    }
}
