package br.ufsc.lapesd.riefederator.rel.sql.impl;

import br.ufsc.lapesd.riefederator.model.term.Blank;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.sql.SqlTermWriter;

import javax.annotation.Nonnull;

import static java.util.Collections.singletonList;

public class ExistsSqlSelector extends SqlSelector {
    public ExistsSqlSelector(@Nonnull Column column, @Nonnull Term blankNode) {
        super(singletonList(column), singletonList(blankNode));
        assert blankNode.isBlank();
    }

    public @Nonnull Column getColumn() {
        assert !getColumns().isEmpty();
        return getColumns().get(0);
    }

    public @Nonnull Term getTerm() {
        assert !getSparqlTerms().isEmpty();
        return getSparqlTerms().get(0);
    }

    public @Nonnull Blank getBlank() {
        return getTerm().asBlank();
    }

    @Override
    public boolean hasSqlCondition() {
        return true;
    }

    @Override
    public @Nonnull String getSqlCondition(@Nonnull SqlTermWriter writer) {
        return getColumn() + " IS NOT NULL";
    }
}
