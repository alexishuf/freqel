package br.ufsc.lapesd.freqel.reason.tbox.replacements.pruners;

import br.ufsc.lapesd.freqel.description.CQueryMatch;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.description.MatchReasoning;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementContext;
import br.ufsc.lapesd.freqel.reason.tbox.replacements.ReplacementPruner;

import javax.annotation.Nonnull;

public class DescriptionReplacementPruner implements ReplacementPruner {
    @Override
    public boolean shouldPrune(@Nonnull Term original, @Nonnull Term replacement,
                               @Nonnull ReplacementContext ctx) {
        Description description = ctx.getEndpoint().getDescription();
        CQuery bound = ctx.getSubQuery();
        if (!replacement.equals(original))
            bound = bound.bind(t -> t.equals(original) ? replacement : t);
        CQueryMatch match = description.localMatch(bound, MatchReasoning.NONE);
        return !match.getIrrelevant(bound).isEmpty();
    }

    @Override public String toString() {
        return getClass().getSimpleName();
    }
}
