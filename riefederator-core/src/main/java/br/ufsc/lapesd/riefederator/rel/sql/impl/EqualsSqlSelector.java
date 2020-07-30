package br.ufsc.lapesd.riefederator.rel.sql.impl;


import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.rel.common.RelationalTermWriter;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;

import javax.annotation.Nonnull;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;

public class EqualsSqlSelector extends SqlSelector {
    public EqualsSqlSelector(@Nonnull Column column, @Nonnull Term term) {
        super(singletonList(column), singletonList(term));
        sparqlVars = emptySet();
    }

    @Override
    public boolean hasCondition() {
        return true;
    }

    @Override
    public @Nonnull String getCondition(@Nonnull RelationalTermWriter writer) {
        return columns.get(0).toString() + " = " + writer.apply(sparqlTerms.get(0));
    }
}
