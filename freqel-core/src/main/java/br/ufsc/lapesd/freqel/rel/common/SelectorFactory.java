package br.ufsc.lapesd.freqel.rel.common;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.rel.mappings.Column;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

public interface SelectorFactory {
    interface Context {
        @Nonnull CQuery getQuery();
        @Nonnull Collection<Column> getColumns(@Nonnull Term term);
        @Nullable Column getDirectMapped(@Nonnull Term term, @Nullable Triple triple);
    }

    @Nullable Selector create(@Nonnull Context context, @Nonnull SPARQLFilter filter);
    @Nullable Selector create(@Nonnull Context context, @Nonnull Triple filter);
}
