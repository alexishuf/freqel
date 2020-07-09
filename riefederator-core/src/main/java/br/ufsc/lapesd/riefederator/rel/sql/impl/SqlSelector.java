package br.ufsc.lapesd.riefederator.rel.sql.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.sql.DefaultSqlTermWriter;
import br.ufsc.lapesd.riefederator.rel.sql.SqlTermWriter;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public abstract class SqlSelector {
    protected final @Nonnull List<Column> columns;
    protected final @Nonnull List<Term> sparqlTerms;
    protected @LazyInit @Nullable Set<String> sparqlVars;

    public SqlSelector(@Nonnull List<Column> columns, @Nonnull List<Term> sparqlTerms) {
        this.columns = columns;
        this.sparqlTerms = sparqlTerms;
    }

    abstract public boolean hasSqlCondition();
    abstract public @Nonnull String getSqlCondition(@Nonnull SqlTermWriter writer);

    public @Nonnull String getSqlCondition() {
        return getSqlCondition(DefaultSqlTermWriter.INSTANCE);
    }

    public @Nonnull List<Column> getColumns() {
        return columns;
    }
    public @Nonnull List<Term> getSparqlTerms() {
        return sparqlTerms;
    }
    public @Nonnull Set<String> getSparqlVars() {
        if (sparqlVars == null) {
            sparqlVars = sparqlTerms.stream().filter(Term::isVar)
                                             .map(t -> t.asVar().getName()).collect(toSet());
        }
        return sparqlVars;
    }

    @Override
    public String toString() {
        return hasSqlCondition() ? getSqlCondition()
                        : getSparqlVars().stream().map(v -> "?"+v).collect(Collectors.joining());
    }
}
