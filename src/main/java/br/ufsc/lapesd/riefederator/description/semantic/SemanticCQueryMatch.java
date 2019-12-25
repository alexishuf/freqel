package br.ufsc.lapesd.riefederator.description.semantic;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.WillClose;
import java.util.Collection;
import java.util.Set;

@Immutable
public class SemanticCQueryMatch extends CQueryMatch {
    private final @Nonnull ImmutableSetMultimap<Triple, CQuery> alternatives;
    private final @Nonnull ImmutableSetMultimap<CQuery, CQuery> exgAlternatives;

    public SemanticCQueryMatch(@Nonnull CQuery query) {
        super(query);
        alternatives = ImmutableSetMultimap.of();
        exgAlternatives = ImmutableSetMultimap.of();
    }

    public SemanticCQueryMatch(@Nonnull CQuery query,
                               @Nonnull ImmutableList<CQuery> exclusiveGroups,
                               @Nonnull ImmutableList<Triple> nonExclusiveRelevant,
                               @Nonnull ImmutableSetMultimap<Triple, CQuery> alternatives,
                               @Nonnull ImmutableSetMultimap<CQuery, CQuery> exgAlternatives) {
        super(query, exclusiveGroups, nonExclusiveRelevant);
        this.alternatives = alternatives;
        this.exgAlternatives = exgAlternatives;
    }

    public static class Builder extends CQueryMatch.Builder {
        private @Nonnull final ImmutableSetMultimap.Builder<Triple, CQuery> alternatives;
        private @Nonnull final ImmutableSetMultimap.Builder<CQuery, CQuery> exgAlternatives;

        public Builder(@Nonnull CQuery query) {
            super(query);
            alternatives = ImmutableSetMultimap.builder();
            exgAlternatives = ImmutableSetMultimap.builder();
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder addAlternative(@Nonnull Triple query,
                                               @Nonnull CQuery alternative) {
            if (getClass().desiredAssertionStatus())
                Preconditions.checkArgument(this.query.contains(query), "triple not in query");
            alternatives.put(query, alternative);
            return this;
        }

        @Override @CanIgnoreReturnValue
        public @Nonnull Builder addExclusiveGroup(@Nonnull Collection<Triple> group) {
            super.addExclusiveGroup(group);
            return this;
        }

        @Override @CanIgnoreReturnValue
        public @Nonnull Builder addTriple(@Nonnull Triple triple) {
            super.addTriple(triple);
            return this;
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder addAlternative(@Nonnull Triple query, @Nonnull Triple alternative) {
            return addAlternative(query, CQuery.from(alternative));
        }

        @CanIgnoreReturnValue
        public @Nonnull Builder addAlternative(@Nonnull CQuery exGroup,
                                               @Nonnull CQuery alternative) {
            if (getClass().desiredAssertionStatus())
                Preconditions.checkArgument(query.containsAll(exGroup), "exGroup not in query");
            exgAlternatives.put(exGroup, alternative);
            return this;
        }

        @Override @WillClose
        public @Nonnull SemanticCQueryMatch build() {
            Preconditions.checkState(!built);
            built = true;
            return new SemanticCQueryMatch(query, exclusiveGroupsBuilder.build(),
                                           nonExclusiveBuilder.build(),
                                           alternatives.build(), exgAlternatives.build());
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
