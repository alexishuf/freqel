package br.ufsc.lapesd.riefederator.description;

import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;

public interface Description {
    /**
     * Gets a {@link CQueryMatch} for the subset of query that matches this {@link Description};
     */
    @Nonnull CQueryMatch match(@Nonnull CQuery query);
}
