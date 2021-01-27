package br.ufsc.lapesd.freqel.rel.common;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.freqel.rel.mappings.Column;
import br.ufsc.lapesd.freqel.rel.mappings.tags.ColumnsTag;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
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

    private final @Nonnull IndexSubset<Triple> triples;
    private final @Nonnull IndexSubset<String> varNames;
    private final @Nonnull IndexSubset<SPARQLFilter> filters;
    private final @Nonnull CQuery query;
    private @LazyInit @Nullable String table;

    public StarSubQuery(@Nonnull IndexSubset<Triple> triples,
                        @Nonnull IndexSubset<String> varNames,
                        @Nonnull IndexSubset<SPARQLFilter> filters,
                        @Nonnull CQuery query) {
        Preconditions.checkArgument(!triples.isEmpty());
        this.triples = triples;
        this.varNames = varNames;
        this.filters = filters;
        this.query = query;
        assert triples.stream().map(Triple::getSubject).allMatch(getCore()::equals);
    }

    public @Nonnull IndexSubset<Triple> getTriples() {
        return triples;
    }
    public @Nonnull IndexSubset<String> getVarNames() {
        return varNames;
    }
    public @Nonnull IndexSubset<SPARQLFilter> getFilters() {
        return filters;
    }
    public @Nonnull IndexSet<SPARQLFilter> getAllQueryFilters() {
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
        return query.getModifiers().distinct() != null;
    }

    public @Nonnull Set<Column> getAllColumns(@Nonnull Term term) {
        return StarsHelper.getColumns(query, findTable(), term);
    }

    public @Nullable ColumnsTag getColumnsTag(@Nonnull Term term) {
        String table = findTable();
        if (table == null) {
            assert false : "Star has no table";
            return null;
        }
        return StarsHelper.getColumnsTag(query, table, term, triples);
    }

    public @Nullable String findTable() {
        if (this.table != null)
            return this.table;
        if (triples.isEmpty()) {
            assert false : "Empty star!";
            return null;
        }
        Term core = getCore();
        assert triples.stream().allMatch(t -> t.getSubject().equals(core));
        table = StarsHelper.findTable(query, core);
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
