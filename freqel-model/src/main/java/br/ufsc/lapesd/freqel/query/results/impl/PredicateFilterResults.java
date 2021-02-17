package br.ufsc.lapesd.freqel.query.results.impl;

import br.ufsc.lapesd.freqel.query.results.DelegatingResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Predicate;

public class PredicateFilterResults extends DelegatingResults implements Results {
    private @Nullable Solution next;
    private final @Nonnull List<? extends Predicate<Solution>> predicates;
    private final @Nullable ArraySolution.ValueFactory projector;

    public PredicateFilterResults(@Nonnull Results in, Predicate<Solution> predicate) {
        this(in, Collections.singletonList(predicate));
    }

    public PredicateFilterResults(@Nonnull Results in,
                                  @Nonnull List<? extends Predicate<Solution>> predicates) {
        super(in.getVarNames(), in);
        this.predicates = predicates;
        this.projector = null;
    }

    public PredicateFilterResults(@Nonnull Set<String> varNames, @Nonnull Results in,
                                  @Nonnull Predicate<Solution> predicate) {
        this(varNames, in, Collections.singletonList(predicate));
    }

    public PredicateFilterResults(@Nonnull Set<String> varNames, @Nonnull Results in,
                                  @Nonnull List<? extends Predicate<Solution>> predicates) {
        super(varNames, in);
        this.predicates = predicates;
        this.projector = ArraySolution.forVars(varNames);
    }

    @Override public boolean hasNext() {
        while (next == null && in.hasNext()) {
            Solution solution = in.next();
            boolean ok = true;
            for (Predicate<Solution> predicate : predicates) {
                if (!(ok = predicate.test(solution)))
                    break;
            }
            if (ok)
                next = projector == null ? solution : projector.fromSolution(solution);
        }
        return next != null;
    }

    @Override public @Nonnull Solution next() {
        if (!hasNext()) throw new NoSuchElementException();
        Solution solution = this.next;
        this.next = null;
        assert  solution != null;
        return solution;
    }
}
