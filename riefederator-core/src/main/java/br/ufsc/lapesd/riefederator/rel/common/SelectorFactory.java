package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;

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
