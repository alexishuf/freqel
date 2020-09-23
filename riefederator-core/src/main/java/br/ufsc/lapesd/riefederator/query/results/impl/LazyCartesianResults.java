package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.*;
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.subset.IndexSubset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class LazyCartesianResults extends AbstractResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(LazyCartesianResults.class);
    private static final Set<Solution> EMPTY_SINGLETON = singleton(ArraySolution.EMPTY);

    private final @Nonnull ResultsList<Results> inputs;
    private @Nullable ResultsList<Results> emptyOptionals = null;
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
        boolean first = true;
        for (Results in : ins) {
            if (first) {
                first = false;
                Results dr = parallelFactory.apply(WindowDistinctResults.applyIfNotDistinct(in));
                this.inputs.add(dr);
            } else {
                Results dr = parallelFactory.apply(HashDistinctResults.applyIfNotDistinct(in));
                this.inputs.add(ListBufferedResults.applyIfNotBuffered(dr));
            }
        }
        factory = ArraySolution.forVars(getVarNames());

        // check varNames and non-joinable components
        if (LazyCartesianResults.class.desiredAssertionStatus()) {
            IndexSet<String> all = FullIndexSet.fromDistinct(
                    ins.stream().flatMap(i -> i.getVarNames().stream()).collect(toSet()));
            assert all.containsAll(varNames) : "Some result variables not found in any input";
            List<IndexSubset<String>> varSets =
                    ins.stream().map(r -> all.subset(r.getVarNames())).collect(toList());
            assert varSets.size() == size;
            for (int i = 0; i < size; i++) {
                IndexSubset<String> left = varSets.get(i);
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
            int inReadyCount = in.getReadyCount();
            if (inReadyCount == 0)
                return 0;
            int next = ready * inReadyCount;
            if (next < ready) {
                logger.warn("LazyCartesianResults {} has so many results it overflows", this);
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
            Results r = inputs.get(i);
            assert r != null;
            if (r.hasNext()) {
                solutions[i] = r.next();
                return true; // done, can assemble a new solution
            } else if (i > 0) {
                ((BufferedResults) r).reset(true);
                solutions[i] = r.next();
                // continue iteration
            }
        }
        exhausted = true; // no new solutions
        return false;
    }

    private boolean initSolutions() {
        assert !initialized;
        initialized = true;
        int size = inputs.size();
        assert solutions.length == size;
        for (int i = 0; i < size; i++) {
            Results r = inputs.get(i);
            if (r.hasNext()) {
                solutions[i] = r.next();
            } else if (r.isOptional()) {
                if (emptyOptionals == null)
                    emptyOptionals = new ResultsList<>();
                emptyOptionals.add(r); // save for later close()
                CollectionResults filler = new CollectionResults(EMPTY_SINGLETON, r.getVarNames());
                inputs.set(i, filler);
                solutions[i] = filler.next();
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
