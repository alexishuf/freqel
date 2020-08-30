package br.ufsc.lapesd.riefederator.federation.planner.pre.steps;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

public class StepUtils {
    public static void exposeFilterVars(@Nonnull Op op) {
        Projection projection = op.modifiers().projection();
        if (projection != null) {
            Set<String> ok = projection.getVarNames();
            Set<String> required = null;
            for (SPARQLFilter filter : op.modifiers().filters()) {
                for (String var : filter.getVarTermNames()) {
                    if (!ok.contains(var))
                        (required == null ? required = new HashSet<>() : required).add(var);
                }
            }
            if (required != null) {
                required.addAll(ok);
                op.modifiers().add(Projection.of(required));
            }
        }
    }
}
