package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;

import javax.annotation.Nonnull;
import java.util.List;

public interface Selector {
    boolean hasCondition();
    @Nonnull String getCondition();
    @Nonnull List<Column> getColumns();
    @Nonnull List<Term> getTerms();
}
