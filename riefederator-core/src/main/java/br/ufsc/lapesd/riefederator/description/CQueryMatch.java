package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.query.CQuery;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.WillClose;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The result of matching a conjunctive query against a {@link Description}.
 */
@SuppressWarnings("UnstableApiUsage")
@Immutable
public class CQueryMatch {
    private final @Nonnull ImmutableList<CQuery> exclusiveGroups;
    private final @Nonnull ImmutableList<Triple> nonExclusiveRelevant;
    private final @Nonnull CQuery query;
    @SuppressWarnings("Immutable")
    private @Nonnull SoftReference<ImmutableList<Triple>> allRelevant = new SoftReference<>(null);

    public CQueryMatch(@Nonnull CQuery query) {
        this(query, ImmutableList.of(), ImmutableList.of());
    }

    public CQueryMatch(@Nonnull CQuery query,
                       @Nonnull ImmutableList<CQuery> exclusiveGroups,
                       @Nonnull ImmutableList<Triple> nonExclusiveRelevant) {
        this.exclusiveGroups = exclusiveGroups;
        this.nonExclusiveRelevant = nonExclusiveRelevant;
        this.query = query;
    }

    public static class Builder {
        protected final @Nonnull CQuery query;
        protected final @Nonnull ImmutableList.Builder<CQuery> exclusiveGroupsBuilder;
        protected final @Nonnull ImmutableList.Builder<Triple> nonExclusiveBuilder;
        private final @Nonnull HashSet<CQuery> exclusiveGroups;
        private final @Nonnull HashSet<Triple> nonExclusive;
        protected boolean built = false;

        @SuppressWarnings("UnstableApiUsage")
        protected Builder(@Nonnull CQuery query) {
            this.query = query;
            int capacity = Math.max(query.size() / 2, 10);
            exclusiveGroupsBuilder = ImmutableList.builderWithExpectedSize(capacity);
            nonExclusiveBuilder = ImmutableList.builderWithExpectedSize(capacity);
            exclusiveGroups = new HashSet<>(capacity);
            nonExclusive = new HashSet<>(capacity);
        }

        @Contract("_ -> this")
        public @Nonnull Builder addExclusiveGroup(@Nonnull Collection<Triple> group) {
            Preconditions.checkState(!built);
            if (CQueryMatch.class.desiredAssertionStatus()) {
                checkArgument(query.containsAll(group), "Triple in group not in query");
            }
            checkArgument(!group.isEmpty(), "Exclusive group cannot be empty");
            CQuery cQuery = CQuery.from(group);
            if (exclusiveGroups.add(cQuery))
                exclusiveGroupsBuilder.add(cQuery);
            return this;
        }

        @Contract("_ -> this")
        public @Nonnull Builder addTriple(@Nonnull Triple triple) {
            Preconditions.checkState(!built);
            if (CQueryMatch.class.desiredAssertionStatus())
                checkArgument(query.contains(triple), "Triple not in query");
            if (nonExclusive.add(triple))
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
        ImmutableList<Triple> strong = allRelevant.get();
        if (strong == null) {
            ImmutableList.Builder<Triple> builder = ImmutableList.builderWithExpectedSize(query.size());
            exclusiveGroups.forEach(builder::addAll);
            builder.addAll(nonExclusiveRelevant);
            allRelevant = new SoftReference<>(strong = builder.build());
        }
        return strong;
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
    public @Nonnull ImmutableList<CQuery> getKnownExclusiveGroups() {
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

