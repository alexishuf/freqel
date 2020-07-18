package br.ufsc.lapesd.riefederator.rel.sql.impl;

import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.sql.SqlTermWriter;

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
    public @Nonnull String getCondition(@Nonnull SqlTermWriter writer) {
        return "";
    }
}
