package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.WillClose;
import java.util.*;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableList;

/**
 * The result of matching a conjunctive query against a {@link Description}.
 */
public class CQueryMatch {
    private @Nonnull List<List<Triple>> exclusiveGroups;
    private @Nonnull List<Triple> nonExclusiveRelevant;
    private @Nonnull List<Triple> query;

    public CQueryMatch(@Nonnull List<Triple> query, @Nonnull List<List<Triple>> exclusiveGroups,
                       @Nonnull List<Triple> nonExclusiveRelevant) {
        for (int i = 0; i < exclusiveGroups.size(); i++)
            exclusiveGroups.set(i, unmodifiableList(exclusiveGroups.get(i)));
        this.exclusiveGroups = unmodifiableList(exclusiveGroups);
        this.nonExclusiveRelevant = unmodifiableList(nonExclusiveRelevant);
        this.query = unmodifiableList(query);
    }

    public static class Builder {
        private final @Nonnull List<Triple> query;
        private final @Nonnull ArrayList<List<Triple>> exclusiveGroups;
        private final @Nonnull ArrayList<Triple> nonExclusive;
        private boolean built = false;

        private Builder(@Nonnull List<Triple> query) {
            this.query = query;
            int capacity = Math.max(query.size() / 2, 10);
            exclusiveGroups = new ArrayList<>(capacity);
            nonExclusive = new ArrayList<>(capacity);
        }

        @Contract("_ -> this")
        public @Nonnull Builder addExclusiveGroup(@Nonnull List<Triple> group) {
            Preconditions.checkState(!built);
            if (CQueryMatch.class.desiredAssertionStatus()) {
                Preconditions.checkArgument(query.containsAll(group),
                                            "Triple in group not in query");
            }
            Preconditions.checkArgument(!group.isEmpty(), "Exclusive group cannot be empty");
            exclusiveGroups.add(group);
            return this;
        }

        @Contract("_ -> this")
        public @Nonnull Builder addTriple(@Nonnull Triple triple) {
            Preconditions.checkState(!built);
            if (CQueryMatch.class.desiredAssertionStatus())
                Preconditions.checkArgument(query.contains(triple), "Triple not in query");
            nonExclusive.add(triple);
            return this;
        }

        @WillClose
        public @Nonnull CQueryMatch build() {
            Preconditions.checkState(!built);
            built = true;
            return new CQueryMatch(query, exclusiveGroups, nonExclusive);
        }
    }

    public static @Nonnull Builder builder(@Nonnull List<Triple> query) {
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
    public @Nonnull List<Triple> getAllRelevant() {
        int capacity = query.size();
        ArrayList<Triple> list = new ArrayList<>(capacity);
        exclusiveGroups.forEach(list::addAll);
        list.addAll(nonExclusiveRelevant);
        return unmodifiableList(list);
    }

    /**
     * Gets all triples which are not in <code>getAllRelevant()</code>.
     * @return An unmodifiable List with the triples
     */
    public @Nonnull List<Triple> getIrrelevant() {
        Set<Triple> set = new LinkedHashSet<>(query);
        Stream.concat(exclusiveGroups.stream().flatMap(Collection::stream),
                      nonExclusiveRelevant.stream()).forEach(set::remove);
        return new ArrayList<>(set);
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
    public @Nonnull List<List<Triple>> getKnownExclusiveGroups() {
        return exclusiveGroups;
    }

    @Override
    public @Nonnull String toString() { return toString(StdPrefixDict.STANDARD); }

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
