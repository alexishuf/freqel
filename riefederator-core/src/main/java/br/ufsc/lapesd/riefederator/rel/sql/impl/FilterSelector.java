package br.ufsc.lapesd.riefederator.rel.sql.impl;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.sql.SqlTermWriter;

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
    public @Nonnull String getCondition(@Nonnull SqlTermWriter writer) {
        return sqlCondition;
    }
}
