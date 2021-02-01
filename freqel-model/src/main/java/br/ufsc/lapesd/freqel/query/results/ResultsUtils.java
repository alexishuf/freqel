package br.ufsc.lapesd.freqel.query.results;

import br.ufsc.lapesd.freqel.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.freqel.query.results.impl.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ResultsUtils {
    public static @Nonnull Results applyModifiers(@Nonnull Results in,
                                                  @Nullable ModifiersSet modifiers) {
        if (modifiers == null)
            return in;
        Results r = SPARQLFilterResults.applyIf(in, modifiers.filters());
        return applyNonFilterModifiers(r, modifiers);
    }

    public static @Nonnull Results applyNonFilterModifiers(@Nonnull Results in,
                                                           @Nullable ModifiersSet modifiers) {
        if (modifiers == null)
            return in;
        Results r = ProjectingResults.applyIf(in, modifiers);
        r = HashDistinctResults.applyIf(r, modifiers);
        r = LimitResults.applyIf(r, modifiers);
        r = AskResults.applyIf(r, modifiers);
        if (modifiers.optional() != null)
            r.setOptional(true);
        return r;
    }
}
