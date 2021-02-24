package br.ufsc.lapesd.freqel.reason.tbox.endpoint;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.PredicateFilterResults;
import br.ufsc.lapesd.freqel.query.results.impl.ThenResults;
import br.ufsc.lapesd.freqel.reason.tbox.EndpointReasoner;
import br.ufsc.lapesd.freqel.reason.tbox.TBox;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.Replacement;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementGenerator;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.VarReplacement;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

public class HeuristicEndpointReasoner implements EndpointReasoner {
    private static final Logger logger = LoggerFactory.getLogger(HeuristicEndpointReasoner.class);

    private static final int DEF_ESTIMATION_THRESHOLD = 3;
    private static final Var hVar = new StdVar( "HeuristicEndpointReasoner_est");
    private static final AtomicInteger nextInstanceId = new AtomicInteger(1);

    private @Nullable TBox tBox;
    private final @Nonnull ReplacementGenerator replacementGenerator;
    private final @Nonnull CardinalityHeuristic cardinalityHeuristic;
    private final @Nonnull String myPrefix =
            "HeuristicEndpointReasoner_" + nextInstanceId.getAndIncrement() + "_";
    private int estimationThreshold = DEF_ESTIMATION_THRESHOLD;

    @Inject
    public HeuristicEndpointReasoner(@Nonnull ReplacementGenerator replacementGenerator,
                                     @Nonnull @Named("fast") CardinalityHeuristic heuristic) {
        this.replacementGenerator = replacementGenerator;
        this.cardinalityHeuristic = heuristic;
    }

    public int getEstimationThreshold() {
        return estimationThreshold;
    }
    public void setEstimationThreshold(int estimationThreshold) {
        this.estimationThreshold = estimationThreshold;
    }

    @Override public void setTBox(@Nonnull TBox tBox) {
        this.tBox = tBox;
        TBox old = replacementGenerator.setTBox(tBox);
        if (old != null && old != tBox)
            logger.info("Replaced TBox {} of {} with {}", old, replacementGenerator, tBox);
    }

    @Override public boolean acceptDisjunctive() {
        return false;
    }

    @VisibleForTesting
    @Nonnull List<Replacement> getReplacements(@Nonnull CQuery query,
                                               @Nonnull TPEndpoint ep) {
        int i = -1;
        List<Replacement> replacements = new ArrayList<>();
        for (Replacement r : replacementGenerator.generate(query, ep)) {
            ++i;
            int nAlts = r.getAlternatives().size();
            if (nAlts > getEstimationThreshold()) {
                Term original = r.getTerm();
                CQuery withVar = r.getCtx().getQuery().bind(t -> t.equals(original) ? hVar : t);
                Cardinality estimate = cardinalityHeuristic.estimate(withVar, ep);
                long optimistic = estimate.getValue(100) / 2 + 1;
                if (nAlts > optimistic) {
                    replacements.add(new VarReplacement(new StdVar(myPrefix + i), r));
                    continue;
                }
            }
            replacements.add(r);
        }
        return replacements;
    }

    private static class RewritingResultsIterator implements Iterator<Callable<Results>> {
        private final @Nonnull Function<EndpointOp, Results> executor;
        private final @Nonnull EndpointQueryOp op;
        private final @Nonnull CQuery originalQuery;
        private final @Nonnull List<Replacement> replacements;
        private final @Nonnull List<IndexSet<Term>> alternativeSets;
        private final @Nonnull List<Iterator<Term>> alternativeIts;
        private final @Nonnull List<Term> alternativeTerms;
        private final @Nonnull Map<Term, Term> bindings;
        private @Nullable Callable<Results> next;
        private boolean exhausted = false;
        private final boolean hasVarBindings;

        public RewritingResultsIterator(@Nonnull EndpointQueryOp op,
                                        @Nonnull Function<EndpointOp, Results> executor,
                                        @Nonnull List<Replacement> replacements) {
            this.op = op;
            this.executor = executor;
            this.replacements = replacements;
            this.bindings = Maps.newHashMapWithExpectedSize(replacements.size());

            int capacity = 0;
            for (Replacement replacement : replacements)
                capacity += replacement.getAlternatives().size();
            FullIndexSet<Term> universe = new FullIndexSet<>(capacity);
            for (Replacement replacement : replacements)
                universe.addAll(replacement.getAlternatives());

            this.alternativeSets = new ArrayList<>(replacements.size());
            this.alternativeIts = new ArrayList<>(replacements.size());
            this.alternativeTerms = new ArrayList<>(replacements.size());
            boolean hasVarBindings = false;
            for (Replacement replacement : replacements) {
                IndexSubset<Term> ss = universe.subset(replacement.getAlternatives());
                Iterator<Term> it = ss.iterator();
                hasVarBindings |= replacement instanceof VarReplacement;
                alternativeSets.add(ss);
                alternativeIts.add(it);
                if (it.hasNext())
                    alternativeTerms.add(it.next());
                else
                    exhausted = true;
            }
            this.hasVarBindings = hasVarBindings;
            if (hasVarBindings) {
                MutableCQuery copy = new MutableCQuery(op.getQuery());
                copy.mutateModifiers().removeIf(Projection.class::isInstance);
                originalQuery = copy;
            } else {
                originalQuery = op.getQuery();
            }
        }

        private @Nonnull Results handleVarBindings(@Nonnull Results results) {
            assert hasVarBindings;
            List<Predicate<Solution>> predicates = new ArrayList<>();
            for (Replacement r : replacements) {
                if (!(r instanceof VarReplacement)) continue;
                VarReplacement vr = (VarReplacement) r;
                Var v = vr.getVar();
                Set<Term> alternatives = vr.getGroundReplacement().getAlternatives();
                predicates.add(s -> alternatives.contains(s.get(v)));
            }
            return new PredicateFilterResults(op.getResultVars(), results, predicates);
        }

        @Override public boolean hasNext() {
            if (next == null && !exhausted) {
                int size = replacements.size();
                for (int i = 0; i < size; i++)
                    bindings.put(replacements.get(i).getTerm(), alternativeTerms.get(i));
                assert bindings.values().stream().allMatch(Objects::nonNull);
                MutableCQuery bound = originalQuery.bind(bindings);
                EndpointQueryOp boundOp = new EndpointQueryOp(op.getEndpoint(), bound);
                if (hasVarBindings)
                    next = () -> handleVarBindings(executor.apply(boundOp));
                else
                    next = () -> executor.apply(boundOp);
                boolean advanced = false;
                for (int i = size-1; !advanced && i >= 0; i--) {
                    Iterator<Term> it = alternativeIts.get(i);
                    if (it.hasNext()) {
                        alternativeTerms.set(i, it.next());
                        advanced = true;
                    } else {
                        Iterator<Term> newIt = alternativeSets.get(i).iterator();
                        alternativeIts.set(i, newIt);
                        alternativeTerms.set(i, newIt.next());
                    }
                }
                if (!advanced)
                    exhausted = true;
            }
            return next != null;
        }

        @Override public @Nonnull Callable<Results> next() {
            if (!hasNext()) throw new NoSuchElementException();
            Callable<Results> callable = this.next;
            this.next = null;
            assert callable != null;
            return callable;
        }
    }

    @Override
    public @Nonnull Results apply(@Nonnull EndpointOp query,
                                  @Nonnull Function<EndpointOp, Results> executor) {
        if (tBox == null)
            return executor.apply(query);
        if (!(query instanceof EndpointQueryOp))
            throw new IllegalArgumentException(this+" only supports EndpointQueryOp");
        assert query.modifiers().reasoning() == null : "query should not have a REASONING mod";

        MutableCQuery cQuery = ((EndpointQueryOp) query).getQuery();
        List<Replacement> replacements = getReplacements(cQuery, query.getEndpoint());
        if (replacements.isEmpty())
            return executor.apply(query);
        RewritingResultsIterator it =
                new RewritingResultsIterator((EndpointQueryOp) query, executor, replacements);
        return new ThenResults(query.getResultVars(), it);
    }

    @Override public @Nonnull String toString() {
        String tBoxString = Objects.toString(tBox);
        if (tBoxString.length() > 40)
            tBoxString = tBoxString.substring(0, 40);
        String heuristicString = cardinalityHeuristic.toString();
        if (heuristicString.length() > 40)
            heuristicString = heuristicString.substring(0, 40);
        String genString = replacementGenerator.toString();
        if (genString.length() > 40)
            genString = genString.substring(0, 40);
        return String.format("%s{tBox=%s,heuristic=%s,generator=%s}",
                             HeuristicEndpointReasoner.class.getSimpleName(), tBoxString,
                             heuristicString, genString);
    }
}
