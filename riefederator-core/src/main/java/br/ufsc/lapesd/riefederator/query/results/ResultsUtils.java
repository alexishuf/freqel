package br.ufsc.lapesd.riefederator.query.results;

import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class ResultsUtils {
    public static @Nonnull Set<String> getResultVars(@Nonnull CQuery query) {
        if (query.isAsk())
            return Collections.emptySet();
        Projection p = ModifierUtils.getFirst(Projection.class, query.getModifiers());
        if (p != null) return p.getVarNames();
        return query.getTermVars().stream().map(Var::getName).collect(toSet());
    }
}
