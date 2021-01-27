package br.ufsc.lapesd.freqel.rel.sql.impl;

import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.rel.common.RelationalTermWriter;
import br.ufsc.lapesd.freqel.rel.mappings.Column;

import javax.annotation.Nonnull;
import java.util.Collections;

public class AssignSqlSelector extends SqlSelector {
    public AssignSqlSelector(@Nonnull Column column, @Nonnull String sparqlVar) {
        super(Collections.singletonList(column), Collections.singletonList(new StdVar(sparqlVar)));
    }
    public AssignSqlSelector(@Nonnull Column column, @Nonnull Var sparqlVar) {
        super(Collections.singletonList(column), Collections.singletonList(sparqlVar));
    }

    public @Nonnull String getSparqlVar() {
        //noinspection AssertWithSideEffects
        assert getSparqlVars().size() == 1;
        return getSparqlVars().iterator().next();
    }

    public @Nonnull Column getColumn() {
        assert getColumns().size() == 1;
        return getColumns().get(0);
    }

    @Override
    public boolean hasCondition() {
        return false;
    }
    @Override
    public @Nonnull String getCondition(@Nonnull RelationalTermWriter writer) {
        return "";
    }
}
