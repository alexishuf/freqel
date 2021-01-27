package br.ufsc.lapesd.freqel.rel.sql.impl;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.rel.common.RelationalTermWriter;
import br.ufsc.lapesd.freqel.rel.common.Selector;
import br.ufsc.lapesd.freqel.rel.mappings.Column;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public abstract class SqlSelector implements Selector {
    protected final @Nonnull List<Column> columns;
    protected final @Nonnull List<Term> sparqlTerms;
    protected @LazyInit @Nullable Set<String> sparqlVars;

    public SqlSelector(@Nonnull List<Column> columns, @Nonnull List<Term> sparqlTerms) {
        this.columns = columns;
        this.sparqlTerms = sparqlTerms;
    }

    abstract public boolean hasCondition();
    abstract public @Nonnull String getCondition(@Nonnull RelationalTermWriter writer);

    public @Nonnull String getCondition() {
        return getCondition(DefaultSqlTermWriter.INSTANCE);
    }

    public @Nonnull List<Column> getColumns() {
        return columns;
    }
    public @Nonnull List<Term> getTerms() {
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
        return hasCondition() ? getCondition()
                        : getSparqlVars().stream().map(v -> "?"+v).collect(Collectors.joining());
    }
}
