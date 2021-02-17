package br.ufsc.lapesd.freqel.reason.tbox.replacements.pruners;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementContext;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementPruner;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import java.util.Set;

public class MultiReplacementPruner implements ReplacementPruner {
    private final @Nonnull Set<ReplacementPruner> pruners;

    @Inject public MultiReplacementPruner(@Nonnull Set<ReplacementPruner> pruners) {
        this.pruners = pruners;
    }

    @Override public boolean shouldPrune(@Nonnull Term original, @Nonnull Term replacement,
                                         @Nonnull ReplacementContext ctx) {
        for (ReplacementPruner pruner : pruners) {
            if (pruner.shouldPrune(original, replacement, ctx))
                return true;
        }
        return false;
    }

    @Override public @Nonnull String toString() {
        return "MultiReplacementPruner{"+pruners+"}";
    }
}
