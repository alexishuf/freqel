package br.ufsc.lapesd.riefederator.rel.sql;

import br.ufsc.lapesd.riefederator.model.term.Term;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

public interface SqlTermWriter extends Function<Term, String> {
    /**
     * Convert a RDF term into a SQL string or null if the term is not representable in SQL.
     */
    @Override
    @Nullable String apply(@Nonnull Term term);
}
