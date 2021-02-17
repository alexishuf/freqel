package br.ufsc.lapesd.freqel.reason.tbox.replacements.generators;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.Replacement;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementContext;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementGenerator;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementPruner;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.pruners.NoReplacementPruner;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;

public abstract class PruningReplacementGenerator implements ReplacementGenerator {
    private final @Nonnull ReplacementPruner pruner;

    @Inject public PruningReplacementGenerator(@Nonnull ReplacementPruner pruner) {
        this.pruner = pruner;
    }

    public PruningReplacementGenerator() {
        this(NoReplacementPruner.INSTANCE);
    }

    @Override
    public @Nonnull Iterable<Replacement> generate(@Nonnull CQuery query,
                                                   @Nonnull TPEndpoint endpoint) {
        return new Iterable<Replacement>() {
            @Override public @Nonnull Iterator<Replacement> iterator() {
                return new ReplacementIterator(query, getContexts(query, endpoint));
            }
        };
    }

    @Override
    public @Nonnull List<Replacement> generateList(@Nonnull CQuery query,
                                                   @Nonnull TPEndpoint endpoint) {
        ReplacementIterator it = new ReplacementIterator(query, getContexts(query, endpoint));
        ArrayList<Replacement> list = new ArrayList<>();
        while (it.hasNext())
            list.add(it.next());
        return list;
    }

    /* --- --- --- Overridable behavior --- --- --- */

    protected @Nonnull Iterator<ReplacementContext> getContexts(@Nonnull CQuery query,
                                                                @Nonnull TPEndpoint ep) {
        IndexSet<Triple> parent = query.attr().getSet();
        Iterator<Triple> tripleIt = parent.iterator();
        return new Iterator<ReplacementContext>() {
            @Override public boolean hasNext() { return tripleIt.hasNext(); }
            @Override public ReplacementContext next() {
                return new ReplacementContext(query, parent.subset(tripleIt.next()), ep);
            }
        };
    }

    protected abstract @Nonnull Iterator<Term> getAlternatives(@Nonnull Term original,
                                                               @Nonnull ReplacementContext ctx);

    protected class ReplacementIterator implements Iterator<Replacement> {

        private final @Nonnull Iterator<ReplacementContext> ctxIt;
        private @Nullable ReplacementContext ctx;
        private final @Nonnull TermsIt originalIt;
        private @Nullable Replacement nextReplacement;

        public ReplacementIterator(@Nonnull CQuery query,
                                   @Nonnull Iterator<ReplacementContext> ctxIt) {
            this.ctxIt = ctxIt;
            this.originalIt = new TermsIt(query);
        }

        @Override public boolean hasNext() {
            while (nextReplacement == null) {
                while (!originalIt.hasNext()) {
                    if (!ctxIt.hasNext())
                        return false; // exhausted contexts, end iteration
                    ctx = ctxIt.next();
                    originalIt.setTriples(ctx.getTriples());
                }
                assert ctx != null;
                Term original = originalIt.next();
                Iterator<Term> altIt = getAlternatives(original, ctx);
                Set<Term> survivors = new HashSet<>();
                boolean useful = false;
                while (altIt.hasNext()) {
                    Term candidate = altIt.next();
                    if (!pruner.shouldPrune(original, candidate, ctx)) {
                        useful = useful || !original.equals(candidate);
                        survivors.add(candidate);
                    }
                }
                if (useful)
                    nextReplacement = new Replacement(original, survivors, ctx);
            }
            return true;
        }

        @Override public Replacement next() {
            if (!hasNext())
                throw new NoSuchElementException();
            Replacement replacement = nextReplacement;
            nextReplacement = null;
            return replacement;
        }
    }


    /* --- --- --- Internals --- --- -- */

    private static class TermsIt implements Iterator<Term> {
        private @Nonnull final IndexSubset<Term> visited;
        private Iterator<Triple> tripleIt;
        private Triple.Position nextPosition = Triple.Position.SUBJ;
        private @Nullable Triple triple = null;
        private @Nullable Term nextTerm = null;

        public TermsIt(@Nonnull CQuery query) {
            this.visited = query.attr().allTerms().emptySubset();
            this.tripleIt = Collections.emptyIterator();
        }

        public @Nonnull TermsIt setTriples(@Nonnull Collection<Triple> triples) {
            visited.clear();
            tripleIt = triples.iterator();
            return this;
        }

        private boolean advanceTerm() {
            assert nextTerm == null;
            if (triple == null)
                return false;
            Term term = triple.get(nextPosition);
            if (visited.add(term))
                nextTerm = term;
            nextPosition = Triple.Position.VALUES_LIST.get((nextPosition.ordinal()+1) % 3);
            if (nextPosition == Triple.Position.SUBJ)
                triple = null;
            return nextTerm != null;
        }

        @Override public boolean hasNext() {
            while (nextTerm == null) {
                if (advanceTerm())
                    return true;
                if (!tripleIt.hasNext())
                    return false;
                triple = tripleIt.next();
            }
            return true;
        }

        @Override public @Nonnull Term next() {
            if (!hasNext())
                throw new NoSuchElementException();
            assert nextTerm != null;
            Term term = nextTerm;
            nextTerm = null;
            return term;
        }
    }

}
