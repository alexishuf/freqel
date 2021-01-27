package br.ufsc.lapesd.freqel.rel.common;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.rel.mappings.Column;

import javax.annotation.Nonnull;
import java.util.List;

public interface Selector {
    boolean hasCondition();
    @Nonnull String getCondition();
    @Nonnull List<Column> getColumns();
    @Nonnull List<Term> getTerms();
}
