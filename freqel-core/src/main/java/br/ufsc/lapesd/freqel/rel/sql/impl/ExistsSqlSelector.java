package br.ufsc.lapesd.freqel.rel.sql.impl;

import br.ufsc.lapesd.freqel.model.term.Blank;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.rel.common.RelationalTermWriter;
import br.ufsc.lapesd.freqel.rel.mappings.Column;

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
        assert !getTerms().isEmpty();
        return getTerms().get(0);
    }

    public @Nonnull Blank getBlank() {
        return getTerm().asBlank();
    }

    @Override
    public boolean hasCondition() {
        return true;
    }

    @Override
    public @Nonnull String getCondition(@Nonnull RelationalTermWriter writer) {
        return getColumn() + " IS NOT NULL";
    }
}
