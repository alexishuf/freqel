package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.query.CQuery;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.WillClose;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * The result of matching a conjunctive query against a {@link Description}.
 */
@SuppressWarnings("UnstableApiUsage")
public class CQueryMatch {
    private @Nonnull ImmutableList<ImmutableList<Triple>> exclusiveGroups;
    private @Nonnull ImmutableList<Triple> nonExclusiveRelevant;
    private @Nonnull CQuery query;

    public CQueryMatch(@Nonnull CQuery query) {
        this(query, ImmutableList.of(), ImmutableList.of());
    }

    public CQueryMatch(@Nonnull CQuery query,
                       @Nonnull ImmutableList<ImmutableList<Triple>> exclusiveGroups,
                       @Nonnull ImmutableList<Triple> nonExclusiveRelevant) {
        this.exclusiveGroups = exclusiveGroups;
        this.nonExclusiveRelevant = nonExclusiveRelevant;
        this.query = query;
    }

    public static class Builder {
        private final @Nonnull CQuery query;
        private final @Nonnull ImmutableList.Builder<ImmutableList<Triple>> exclusiveGroupsBuilder;
        private final @Nonnull ImmutableList.Builder<Triple> nonExclusiveBuilder;
        private boolean built = false;

        @SuppressWarnings("UnstableApiUsage")
        private Builder(@Nonnull CQuery query) {
            this.query = query;
            int capacity = Math.max(query.size() / 2, 10);
            exclusiveGroupsBuilder = ImmutableList.builderWithExpectedSize(capacity);
            nonExclusiveBuilder = ImmutableList.builderWithExpectedSize(capacity);
        }

        @Contract("_ -> this")
        public @Nonnull Builder addExclusiveGroup(@Nonnull Collection<Triple> group) {
            Preconditions.checkState(!built);
            if (CQueryMatch.class.desiredAssertionStatus()) {
                Preconditions.checkArgument(query.containsAll(group),
                                            "Triple in group not in query");
            }
            Preconditions.checkArgument(!group.isEmpty(), "Exclusive group cannot be empty");
            exclusiveGroupsBuilder.add(ImmutableList.copyOf(group));
            return this;
        }

        @Contract("_ -> this")
        public @Nonnull Builder addTriple(@Nonnull Triple triple) {
            Preconditions.checkState(!built);
            if (CQueryMatch.class.desiredAssertionStatus())
                Preconditions.checkArgument(query.contains(triple), "Triple not in query");
            nonExclusiveBuilder.add(triple);
            return this;
        }

        @WillClose
        public @Nonnull CQueryMatch build() {
            Preconditions.checkState(!built);
            built = true;
            return new CQueryMatch(query, exclusiveGroupsBuilder.build(),
                                          nonExclusiveBuilder.build());
        }
    }

    public static @Nonnull Builder builder(@Nonnull CQuery query) {
        return new Builder(query);
    }

    /**
     * A match is empty iff it has no relevant triple patterns.
     */
    public boolean isEmpty() {
        return exclusiveGroups.isEmpty() && nonExclusiveRelevant.isEmpty();
    }

    /**
     * The full query for which this {@link CQueryMatch} is a result
     */
    public @Nonnull CQuery getQuery() {
        return query;
    }

    /**
     * Gets all triples in the query for which the source is potentially relevant.
     *
     * @return An unmodifiable List with the triples
     */
    public @Nonnull ImmutableList<Triple> getAllRelevant() {
        ImmutableList.Builder<Triple> builder = ImmutableList.builderWithExpectedSize(query.size());
        exclusiveGroups.forEach(builder::addAll);
        builder.addAll(nonExclusiveRelevant);
        return builder.build();
    }

    /**
     * Gets all triples which are not in <code>getAllRelevant()</code>.
     * @return An unmodifiable List with the triples
     */
    public @Nonnull ImmutableList<Triple> getIrrelevant() {
        Set<Triple> set = new LinkedHashSet<>(query);
        Stream.concat(exclusiveGroups.stream().flatMap(Collection::stream),
                      nonExclusiveRelevant.stream()).forEach(set::remove);
        return ImmutableList.copyOf(set);
    }

    /**
     * Gets the triples in getAllRelevant() which are not part of any exclusive group.
     * @return An unmodifiable {@link List} with the triples.
     */
    public @Nonnull ImmutableList<Triple> getNonExclusiveRelevant() {
        return nonExclusiveRelevant;
    }

    /**
     * Gets a list of the exclusive groups.
     *
     * Exclusive groups are triple patterns which can safely grouped into a single conjunctive
     * query and sent to a source. In general, doing such grouping leads to incomplete results
     * as discussed in [1]. The term exclusive group comes from FedX [2].
     *
     * Note that the other ways (e.g. {@link Atom}<code>.isExclusive()</code>) of creating
     * exclusive groups differ from the method in FedX [2]. The main implication of these extra
     * methods is that exclusive groups may intersect with queries sent to other sources.
     *
     * [1]: https://doi.org/10.3233/SW-140160
     * [2]: http://doi.org/10.1007/978-3-642-25073-6_38
     *
     * @return The list of exclusive groups as an unmodifiable list of unmodifiable lists.
     */
    public @Nonnull ImmutableList<ImmutableList<Triple>> getKnownExclusiveGroups() {
        return exclusiveGroups;
    }

    @Override
    public @Nonnull String toString() { return toString(StdPrefixDict.DEFAULT); }

    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        if (isEmpty()) {
            return "{}";
        }
        StringBuilder b = new StringBuilder(query.size()*32);
        b.append("CQueryMatch{\n");
        for (List<Triple> group : exclusiveGroups) {
            b.append("  EXCLUSIVE_GROUP {\n");
            for (Triple t : group) b.append("    ").append(t.toString(dict)).append(" .\n");
            b.append("  }\n");
        }
        if (!nonExclusiveRelevant.isEmpty()) {
            b.append("  NON_EXCLUSIVE {\n");
            for (Triple t : nonExclusiveRelevant)
                b.append("    ").append(t.toString(dict)).append(" .\n");
            b.append("  }\n");
        }
        b.append("}");
        return b.toString();
    }
}
