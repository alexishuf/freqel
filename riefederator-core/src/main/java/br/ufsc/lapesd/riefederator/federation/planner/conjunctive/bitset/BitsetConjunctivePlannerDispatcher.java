package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.JoinPathsConjunctivePlanner;
import br.ufsc.lapesd.riefederator.query.CQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Collection;

public class BitsetConjunctivePlannerDispatcher implements ConjunctivePlanner {
    private static final Logger logger =
            LoggerFactory.getLogger(BitsetConjunctivePlannerDispatcher.class);
    private final @Nonnull BitsetConjunctivePlanner inputsPlanner;
    private final @Nonnull BitsetNoInputsConjunctivePlanner noInputsPlanner;
    private final @Nonnull JoinPathsConjunctivePlanner fallback;

    @Inject
    public BitsetConjunctivePlannerDispatcher(@Nonnull BitsetConjunctivePlanner inputs,
                                              @Nonnull BitsetNoInputsConjunctivePlanner noInputs,
                                              @Nonnull JoinPathsConjunctivePlanner fallback) {
        this.inputsPlanner = inputs;
        this.noInputsPlanner = noInputs;
        this.fallback = fallback;
        this.noInputsPlanner.setExtractCommonSubsets(false);
    }

    @Override public @Nonnull Op plan(@Nonnull CQuery query, @Nonnull Collection<Op> fragments) {
        boolean bad = query.attr().triplesUniverseOffer() == null
                   || query.attr().varNamesUniverseOffer() == null;
        if (bad) {
            assert false : "Bad test: query misses universe sets";
            logger.warn("{} will fallback to {} due to lacking universes in query", this, fallback);
            return fallback.plan(query, fragments);
        }
        boolean hasInputs = false;
        for (Op op : fragments) {
            if ((hasInputs = op.hasInputs())) break;
        }
        return (hasInputs ? inputsPlanner : noInputsPlanner).plan(query, fragments);
    }

    @Override public @Nonnull String toString() {
        return getClass().getSimpleName();
    }
}
