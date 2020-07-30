package br.ufsc.lapesd.riefederator.rel.cql.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.rel.common.RelationalTermWriter;
import br.ufsc.lapesd.riefederator.rel.common.Selector;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class EqualsCqlSelector implements Selector {
    private final @Nonnull List<Column> columns;
    private final @Nonnull List<Term> terms;
    private final @Nonnull String cql;

    public EqualsCqlSelector(@Nonnull Column column, @Nonnull Term term,
                             @Nonnull String cql) {
        this.columns = Collections.singletonList(column);
        this.terms = Collections.singletonList(term);
        this.cql = cql;
    }

    public static @Nullable EqualsCqlSelector create(@Nonnull Column column,
                                                     @Nonnull Term term,
                                                     @Nonnull RelationalTermWriter termWriter) {
        String termString = termWriter.apply(term);
        if (termString == null)
            return null;
        return new EqualsCqlSelector(column, term, column.getColumn()+" = "+termString);
    }

    @Override
    public boolean hasCondition() {
        return true;
    }

    @Override
    public @Nonnull String getCondition() {
        return cql;
    }

    @Override
    public @Nonnull List<Column> getColumns() {
        return columns;
    }

    @Override
    public @Nonnull List<Term> getTerms() {
        return terms;
    }

    @Override
    public String toString() {
        return getCondition();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EqualsCqlSelector)) return false;
        EqualsCqlSelector that = (EqualsCqlSelector) o;
        return getColumns().equals(that.getColumns()) &&
                getTerms().equals(that.getTerms());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getColumns(), getTerms());
    }
}
