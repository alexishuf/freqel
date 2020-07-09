package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

public class StarSubQuery {
    private static final Logger logger = LoggerFactory.getLogger(StarSubQuery.class);

    private final @Nonnull IndexedSubset<Triple> triples;
    private final @Nonnull Set<String> varNames;
    private final @Nonnull IndexedSubset<SPARQLFilter> filters;
    private final @Nonnull CQuery query;
    private @LazyInit @Nullable String table;

    public StarSubQuery(@Nonnull IndexedSubset<Triple> triples,
                        @Nonnull Set<String> varNames,
                        @Nonnull IndexedSubset<SPARQLFilter> filters,
                        @Nonnull CQuery query) {
        Preconditions.checkArgument(!triples.isEmpty());
        this.triples = triples;
        this.varNames = varNames;
        this.filters = filters;
        this.query = query;
        assert triples.stream().map(Triple::getSubject).allMatch(getCore()::equals);
    }

    public @Nonnull IndexedSubset<Triple> getTriples() {
        return triples;
    }
    public @Nonnull Set<String> getVarNames() {
        return varNames;
    }
    public @Nonnull IndexedSubset<SPARQLFilter> getFilters() {
        return filters;
    }
    public @Nonnull IndexedSet<SPARQLFilter> getAllQueryFilters() {
        return filters.getParent();
    }
    public @Nonnull CQuery getQuery() {
        return query;
    }
    public @Nonnull Term getCore() {
        assert !triples.isEmpty() : "Empty star";
        return triples.iterator().next().getSubject();
    }
    public boolean isCore(@Nonnull String varName) {
        Term core = getCore();
        return core.isVar() && core.asVar().getName().equals(varName);
    }
    public boolean isDistinct() {
        return ModifierUtils.getFirst(Distinct.class, query.getModifiers()) != null;
    }

    public @Nullable Column getColumn(@Nonnull Term term) {
        return getColumn(term, !StarSubQuery.class.desiredAssertionStatus());
    }

    public @Nullable Column getColumn(@Nonnull Term term, boolean forgiveAmbiguity) {
        return StarsHelper.getColumn(query, findTable(forgiveAmbiguity), term, forgiveAmbiguity);
    }

    public @Nullable String findTable() {
        return findTable(!StarSubQuery.class.desiredAssertionStatus());
    }

    public @Nullable String findTable(boolean forgiveAmbiguity) {
        if (this.table != null)
            return this.table;
        if (triples.isEmpty()) {
            assert false : "Empty star!";
            return null;
        }
        Term core = getCore();
        assert triples.stream().allMatch(t -> t.getSubject().equals(core));
        table = StarsHelper.findTable(query, core, forgiveAmbiguity);
        if (table == null)
            logger.warn("Star core has no AtomAnnotation/TableTag");
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StarSubQuery)) return false;
        StarSubQuery that = (StarSubQuery) o;
        return getTriples().equals(that.getTriples()) &&
                getFilters().equals(that.getFilters()) &&
                getQuery().equals(that.getQuery());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTriples(), getFilters(), getQuery());
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append('{');
        for (Triple triple : triples)
            b.append(triple.toString()).append('\n');
        for (SPARQLFilter filter : filters)
            b.append(filter.toString()).append('\n');
        b.append('}');
        return b.toString();
    }
}
