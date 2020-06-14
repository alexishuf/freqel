package br.ufsc.lapesd.riefederator.federation.spec;

import br.ufsc.lapesd.riefederator.util.DictTree;

import javax.annotation.Nonnull;

public class FederationSpecException extends Exception {
    private @Nonnull DictTree tree;

    public FederationSpecException(@Nonnull String message, @Nonnull DictTree tree) {
        super(message);
        this.tree = tree;
    }

    public FederationSpecException(@Nonnull String message, Throwable cause,
                                   @Nonnull DictTree tree) {
        super(message, cause);
        this.tree = tree;
    }

    public @Nonnull DictTree getTree() {
        return tree;
    }
}
