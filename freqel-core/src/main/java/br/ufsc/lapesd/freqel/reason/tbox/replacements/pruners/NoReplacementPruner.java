package br.ufsc.lapesd.freqel.reason.tbox.replacements.pruners;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementContext;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementPruner;

import javax.annotation.Nonnull;
import javax.inject.Inject;

public class NoReplacementPruner implements ReplacementPruner {
    public static final @Nonnull NoReplacementPruner INSTANCE = new NoReplacementPruner();

    @Inject public NoReplacementPruner() { }

    @Override
    public boolean shouldPrune(@Nonnull Term original, @Nonnull Term replacement,
                               @Nonnull ReplacementContext ctx) {
        return false;
    }

    @Override public String toString() {
        return getClass().getSimpleName();
    }
}
