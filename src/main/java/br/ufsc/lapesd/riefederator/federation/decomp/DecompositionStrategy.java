package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;
import java.util.Collection;

public interface DecompositionStrategy {
    void addSource(@Nonnull Source source);
    @Nonnull Collection<QueryNode> decomposeIntoLeaves(@Nonnull CQuery query);
    @Nonnull PlanNode decompose(@Nonnull CQuery query);
}
