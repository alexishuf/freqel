package br.ufsc.lapesd.freqel.reason.tbox.replacements;

import br.ufsc.lapesd.freqel.model.term.Term;

import javax.annotation.Nonnull;

public interface ReplacementPruner {
    /**
     * Indicate whether a candidate replacement term should be dropped from further processing.
     *
     * @param original the original term that will be replaced
     * @param replacement the replacement term
     * @param ctx the context of the replacement
     * @return true iff the candidate replacement should not be considered (i.e., it would
     *         produce no results in the given context). Return false if the pruner cannot
     *         make a decision or if the replacement is useful.
     */
    boolean shouldPrune(@Nonnull Term original, @Nonnull Term replacement,
                        @Nonnull ReplacementContext ctx);
}
