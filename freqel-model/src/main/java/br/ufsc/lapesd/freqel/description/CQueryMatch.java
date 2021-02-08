package br.ufsc.lapesd.freqel.description;

import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.util.indexed.ImmFullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.WillClose;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.*;

/**
 * The result of matching a conjunctive query against a {@link Description}.
 */
@Immutable
public class CQueryMatch {
    private final @Nonnull List<CQuery> exclusiveGroups;
    private final @Nonnull IndexSet<Triple> nonExclusiveRelevant;
    private final @Nonnull IndexSet<Triple> unknown;
    private final @Nullable IndexSet<Triple> triplesUniverse;
    @SuppressWarnings("Immutable")
    private @LazyInit @Nullable Set<Triple> allRelevant = null;

    public static final @Nonnull CQueryMatch EMPTY = new CQueryMatch();

    protected CQueryMatch() {
        this(emptyList(), ImmFullIndexSet.empty(), ImmFullIndexSet.empty(), null);
    }

    public CQueryMatch(@Nonnull List<CQuery> exclusiveGroups,
                       @Nonnull IndexSet<Triple> nonExclusiveRelevant,
                       @Nonnull IndexSet<Triple> unknown,
                       @Nullable IndexSet<Triple> triplesUniverse) {
        this.exclusiveGroups = exclusiveGroups;
        this.nonExclusiveRelevant = nonExclusiveRelevant;
        this.unknown = unknown;
        this.triplesUniverse = triplesUniverse;
    }

    public static class Builder {
        protected final @Nonnull CQuery query;
        protected @Nullable List<CQuery> exclusiveGroups;
        protected @Nullable IndexSet<Triple> nonExclusive;
        protected @Nullable IndexSet<Triple> unknown;
        protected boolean built = false;
        private final int capacity;

        protected Builder(@Nonnull CQuery query) {
            this.query = query;
            capacity = (query.size() >> 1) + 1;
        }

        @Contract("_ -> this")
        public @Nonnull Builder addExclusiveGroup(@Nonnull Collection<Triple> group) {
            Preconditions.checkState(!built);
            if (CQueryMatch.class.desiredAssertionStatus()) {
                checkArgument(query.containsAll(group), "Triple in group not in query");
            }
            checkArgument(!group.isEmpty(), "Exclusive group cannot be empty");
            CQuery cQuery = CQuery.from(group);
            if (exclusiveGroups == null)
                exclusiveGroups = new ArrayList<>(capacity);
            assert !exclusiveGroups.contains(query) : "Group already added";
            exclusiveGroups.add(cQuery);
            return this;
        }

        @Contract("_ -> this")
        public @Nonnull Builder addTriple(@Nonnull Triple triple) {
            Preconditions.checkState(!built);
            if (CQueryMatch.class.desiredAssertionStatus())
                checkArgument(query.contains(triple), "Triple not in query");
            if (nonExclusive == null)
                nonExclusive = query.attr().getSet().emptySubset();
            assert !nonExclusive.contains(triple) : "Triple already added";
            nonExclusive.add(triple);
            return this;
        }

        @Contract("_ -> this")
        public @Nonnull Builder addUnknown(@Nonnull Triple triple) {
            Preconditions.checkState(!built);
            assert query.contains(triple) : "triple not in query";
            (unknown == null ? unknown = query.attr().getSet().emptySubset() : unknown).add(triple);
            return this;
        }

        public @Nonnull Builder allUnknown() {
            unknown = query.attr().getSet().immutableFullSubset();
            return this;
        }

        @WillClose
        public @Nonnull CQueryMatch build() {
            Preconditions.checkState(!built);
            built = true;
            List<CQuery> ex = exclusiveGroups == null ? emptyList()
                                                      : unmodifiableList(exclusiveGroups);
            IndexSet<Triple> ne = nonExclusive != null ? nonExclusive
                                : query.attr().getSet().immutableEmptySubset();
            IndexSet<Triple> unknown = this.unknown != null ? this.unknown
                                     : query.attr().getSet().immutableEmptySubset();
            return new CQueryMatch(ex, ne, unknown, query.attr().triplesUniverseOffer());
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
     * Gets all triples in the query for which the source is potentially relevant.
     *
     * @return An unmodifiable List with the triples
     */
    public @Nonnull Set<Triple> getAllRelevant() {
        if (allRelevant == null) {
            if (triplesUniverse != null) {
                IndexSubset<Triple> universe = triplesUniverse.emptySubset();
                IndexSubset<Triple> ss = universe.emptySubset();
                for (Triple t : nonExclusiveRelevant) ss.safeAdd(t);
                for (CQuery eg : exclusiveGroups) {
                    for (Triple t : eg) ss.safeAdd(t);
                }
                allRelevant = ss.asImmutable();
            } else {
                Set<Triple> ss = new HashSet<>(nonExclusiveRelevant);
                for (CQuery eg : exclusiveGroups)
                    ss.addAll(eg);
                allRelevant = unmodifiableSet(ss);
            }
        }
        return allRelevant;
    }

    /**
     * Gets all triples which are not in <code>getAllRelevant()</code>.
     * @return An unmodifiable List with the triples
     */
    public @Nonnull Collection<Triple> getIrrelevant(@Nonnull CQuery query) {
        IndexSubset<Triple> set = query.attr().getSet().fullSubset();
        set.removeAll(getAllRelevant());
        set.removeAll(getUnknown());
        return set;
    }

    /**
     * Get a set of triples that were in the original query being matched but which
     * could not be confirmed to neither be present or to be not present in the source.
     * This happens, for instance with {@link Description#localMatch(CQuery, MatchReasoning)}
     * calls.
     *
     * @return a non-null but possibly empty set of triples of unknown relevant/irrelevant state
     */
    public @Nonnull IndexSet<Triple> getUnknown() {
        return unknown;
    }

    /**
     * Gets the triples in getAllRelevant() which are not part of any exclusive group.
     * @return An unmodifiable {@link List} with the triples.
     */
    public @Nonnull List<Triple> getNonExclusiveRelevant() {
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
    public @Nonnull List<CQuery> getKnownExclusiveGroups() {
        return exclusiveGroups;
    }

    @Override
    public @Nonnull String toString() { return toString(StdPrefixDict.DEFAULT); }

    public @Nonnull String toString(@Nonnull PrefixDict dict) {
        if (isEmpty()) {
            return "{}";
        }
        StringBuilder b = new StringBuilder();
        b.append("CQueryMatch{\n");
        for (CQuery group : exclusiveGroups) {
            String string = group.toString(dict).replaceAll("\n", "\n    ");
            b.append("  EXCLUSIVE_GROUP").append(string.contains("\n") ? "\n    " : " ")
                                         .append(string).append('\n');
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

