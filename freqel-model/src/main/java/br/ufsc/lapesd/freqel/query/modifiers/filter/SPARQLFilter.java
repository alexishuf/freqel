package br.ufsc.lapesd.freqel.query.modifiers.filter;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.modifiers.Modifier;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;

public interface SPARQLFilter extends Modifier {
    @Nonnull SPARQLFilter withVarsEvaluatedAsUnbound(@Nonnull Collection<String> varTermNames);

    @Override
    @Nonnull Capability getCapability();

    void offerVarsNamesUniverse(@Nonnull IndexSet<String> universe);

    @Nonnull SPARQLFilterNode getExpr();

    boolean isTrivial();

    @Nullable Boolean getTrivialResult();

    @Nonnull String getFilterString();

    @Nonnull String getSparqlFilter();

    @Nonnull Set<Term> getTerms();

    @Nonnull Set<Var> getVars();

    @Nonnull Set<String> getVarNames();

    /**
     * Returns true iff all results that pass evalution of this filter also pass for other.
     */
    SubsumptionResult areResultsSubsumedBy(@Nonnull SPARQLFilter other);

    @Nonnull SPARQLFilter bind(@Nonnull Solution solution);

    @Immutable
    class SubsumptionResult {
        private final @Nullable ImmutableBiMap<Term, Term> subsumed2subsuming;
        private final @Nonnull SPARQLFilter subsumed, subsumer;

        public SubsumptionResult(@Nonnull SPARQLFilter subsumed, @Nonnull SPARQLFilter subsumer,
                                 @Nullable BiMap<Term, Term> biMap) {
            this.subsumed = subsumed;
            this.subsumer = subsumer;
            this.subsumed2subsuming = biMap instanceof ImmutableBiMap
                    ? (ImmutableBiMap<Term, Term>)biMap
                    : (biMap == null ? null : ImmutableBiMap.copyOf(biMap));
        }

        public boolean getValue() {
            return subsumed2subsuming != null;
        }

        public @Nonnull SPARQLFilter getSubsumed() {
            return subsumed;
        }

        public @Nonnull SPARQLFilter getSubsumer() {
            return subsumer;
        }

        public @Nonnull Set<Term> subsumedTerms() {
            return subsumed2subsuming == null ? emptySet() : subsumed2subsuming.keySet();
        }
        public @Nonnull Set<Term> subsumingTerms() {
            return subsumed2subsuming == null ? emptySet() : subsumed2subsuming.inverse().keySet();
        }

        public @Nullable Term getOnSubsumed(@Nonnull Term onSubsumed) {
            return subsumed2subsuming == null ? null : subsumed2subsuming.inverse().get(onSubsumed);
        }
        public @Nullable Term getOnSubsuming(@Nonnull Term onSubsumer) {
            return subsumed2subsuming == null ? null : subsumed2subsuming.get(onSubsumer);
        }

        @Override
        public String toString() {
            return subsumed2subsuming == null ? "NOT_SUBSUMES" : subsumed2subsuming.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SubsumptionResult)) return false;
            SubsumptionResult that = (SubsumptionResult) o;
            return Objects.equals(subsumed2subsuming, that.subsumed2subsuming) &&
                    getSubsumed().equals(that.getSubsumed()) &&
                    getSubsumer().equals(that.getSubsumer());
        }

        @Override
        public int hashCode() {
            return Objects.hash(subsumed2subsuming, getSubsumed(), getSubsumer());
        }
    }
}
