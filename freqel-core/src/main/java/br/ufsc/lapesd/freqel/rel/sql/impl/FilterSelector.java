package br.ufsc.lapesd.freqel.rel.sql.impl;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.rel.common.RelationalTermWriter;
import br.ufsc.lapesd.freqel.rel.mappings.Column;

import javax.annotation.Nonnull;
import java.util.List;

public class FilterSelector extends SqlSelector {
    private @Nonnull final String sqlCondition;

    public FilterSelector(@Nonnull List<Column> columns, @Nonnull List<Term> sparqlVars,
                          @Nonnull String sqlCondition) {
        super(columns, sparqlVars);
        this.sqlCondition = sqlCondition;
    }

    @Override
    public boolean hasCondition() {
        return true;
    }

    @Override
    public @Nonnull String getCondition(@Nonnull RelationalTermWriter writer) {
        return sqlCondition;
    }
}
