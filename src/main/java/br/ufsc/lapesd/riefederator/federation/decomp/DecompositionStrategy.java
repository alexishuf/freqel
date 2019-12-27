package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;

public interface DecompositionStrategy {
    void addSource(@Nonnull Source source);
    @Nonnull PlanNode decompose(@Nonnull CQuery query);
}
