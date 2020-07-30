package br.ufsc.lapesd.riefederator.rel.cql;

import br.ufsc.lapesd.riefederator.rel.common.StarVarIndex;

import javax.annotation.Nonnull;

public class MultiStarException extends Exception {
    private final @Nonnull StarVarIndex index;

    public MultiStarException(@Nonnull StarVarIndex index) {
        super("Query has "+index.getStarCount()+" stars, but CQL can handle only one");
        this.index = index;
    }

    public @Nonnull StarVarIndex getIndex() {
        return index;
    }
}
