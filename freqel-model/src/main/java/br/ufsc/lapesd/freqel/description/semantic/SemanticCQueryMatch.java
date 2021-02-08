package br.ufsc.lapesd.freqel.description.semantic;

import br.ufsc.lapesd.freqel.description.CQueryMatch;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.annotations.MatchAnnotation;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.SetMultimap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

@Immutable
public class SemanticCQueryMatch extends CQueryMatch {
    private static final Logger logger = LoggerFactory.getLogger(SemanticCQueryMatch.class);
    private final @Nonnull SetMultimap<Triple, CQuery> alternatives;
    private final @Nonnull SetMultimap<CQuery, CQuery> exgAlternatives;

    public static final @Nonnull SemanticCQueryMatch EMPTY = new SemanticCQueryMatch();

    protected SemanticCQueryMatch() {
        alternatives = ImmutableSetMultimap.of();
        exgAlternatives = ImmutableSetMultimap.of();
    }

    public SemanticCQueryMatch(@Nonnull List<CQuery> exclusiveGroups,
                               @Nonnull IndexSet<Triple> nonExclusiveRelevant,
                               @Nonnull IndexSet<Triple> unknown,
                               @Nonnull SetMultimap<Triple, CQuery> alternatives,
                               @Nonnull SetMultimap<CQuery, CQuery> exgAlternatives,
                               @Nullable IndexSet<Triple> triplesUniverse) {
        super(exclusiveGroups, nonExclusiveRelevant, unknown, triplesUniverse);
        this.alternatives = alternatives;
        this.exgAlternatives = exgAlternatives;
    }

    public static class Builder extends CQueryMatch.Builder {
        private @Nullable SetMultimap<Triple, CQuery> alternatives;
        private @Nullable SetMultimap<CQuery, CQuery> exgAlternatives;
        private final int mmCapacity;

        public Builder(@Nonnull CQuery query) {
            super(query);
            mmCapacity = Math.max(query.size() >> 1, 10);
            alternatives = MultimapBuilder.hashKeys(mmCapacity).hashSetValues().build();
            exgAlternatives = MultimapBuilder.hashKeys(mmCapacity).hashSetValues().build();
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder addAlternative(@Nonnull Triple query,
                                               @Nonnull CQuery alternative) {
            if (getClass().desiredAssertionStatus())
                Preconditions.checkArgument(this.query.contains(query), "triple not in query");
            if (alternatives == null)
                alternatives = MultimapBuilder.hashKeys(mmCapacity).hashSetValues().build();
            alternatives.put(query, alternative);
            return this;
        }

        @Override @CanIgnoreReturnValue
        public @Nonnull Builder addExclusiveGroup(@Nonnull Collection<Triple> group) {
            return (Builder) super.addExclusiveGroup(group);
        }

        @Override @CanIgnoreReturnValue
        public @Nonnull Builder addTriple(@Nonnull Triple triple) {
            return (Builder) super.addTriple(triple);
        }

        @Override public @Nonnull Builder addUnknown(@Nonnull Triple triple) {
            return (Builder) super.addUnknown(triple);
        }

        @Override public @Nonnull Builder allUnknown() {
            return (Builder) super.allUnknown();
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder addAlternative(@Nonnull Triple query, @Nonnull Triple alternative) {
            MutableCQuery cQuery = MutableCQuery.from(alternative);
            if (!alternative.equals(query))
                cQuery.annotate(alternative, new MatchAnnotation(query));
            return addAlternative(query, cQuery);
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder addAlternative(@Nonnull CQuery exGroup,
                                               @Nonnull CQuery alternative) {
            if (getClass().desiredAssertionStatus()) {
                Preconditions.checkArgument(query.containsAll(exGroup), "exGroup not in query");
                boolean hasAnnotation = alternative.hasTripleAnnotations(MatchAnnotation.class);
                if (!hasAnnotation && !exGroup.containsAll(alternative)) {
                    logger.info("addAlternative({}, {}) has new triples without MatchAnnotations",
                                exGroup, alternative);
                }
            }
            if (exgAlternatives == null)
                exgAlternatives = MultimapBuilder.hashKeys(mmCapacity).hashSetValues().build();
            exgAlternatives.put(exGroup, alternative);
            return this;
        }

        @Override @WillClose
        public @Nonnull SemanticCQueryMatch build() {
            Preconditions.checkState(!built);
            built = true;
            List<CQuery> ex = exclusiveGroups == null ? emptyList()
                                                      : unmodifiableList(exclusiveGroups);
            IndexSet<Triple> ne = nonExclusive != null ? nonExclusive
                                : query.attr().getSet().immutableEmptySubset();
            IndexSet<Triple> unknown = this.unknown != null ? this.unknown
                                     : query.attr().getSet().immutableFullSubset();
            SetMultimap<Triple, CQuery> alt = alternatives == null ? ImmutableSetMultimap.of()
                                                                   : alternatives;
            SetMultimap<CQuery, CQuery> exAlt = this.exgAlternatives == null
                                              ? ImmutableSetMultimap.of() : exgAlternatives;
            IndexSet<Triple> universe = query.attr().triplesUniverseOffer();
            return new SemanticCQueryMatch(ex, ne, unknown, alt, exAlt, universe);
        }
    }

    public static @Nonnull Builder builder(@Nonnull CQuery query) {
        return new Builder(query);
    }

    /**
     * Gets the set of alternative queries for the given triple.
     *
     * Each alternative is a {@link CQuery}, with possibly more than one triple. Variable
     * names are preserved so that solutions for the alternative are also solutions for
     * the original {@link Triple}. Note that the result sets for the alternatives may
     * overlap, leading to duplicate results.
     *
     * @return a set of {@link CQuery} alternatives to triple. Empty if triple is not in
     *         {@link #getNonExclusiveRelevant()}()
     */
    public @Nonnull Set<CQuery> getAlternatives(@Nonnull Triple triple) {
        return alternatives.get(triple);
    }

    /**
     * Gets the set of alternative queries for the given exclusive group.
     *
     * The same guarantees of the {@link Triple} overload hold.
     *
     * @return A {@link Set} of {@link CQuery} alternatives or the empty set if exGroup is not in
     *         {@link #getKnownExclusiveGroups()}
     */
    public @Nonnull Set<CQuery> getAlternatives(@Nonnull CQuery exGroup) {
        return exgAlternatives.get(exGroup);
    }
}
