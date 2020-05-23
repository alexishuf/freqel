package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.proto.ProtoQueryNode;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Assign {@link SPARQLFilter}s of a query to the deepest possible node in plans.
 */
class FilterAssigner {
    private final @Nonnull Set<SPARQLFilter> filters = new HashSet<>();
    private final @Nonnull SetMultimap<Term, SPARQLFilter> term2filter;
    private final @Nonnull Function<ProtoQueryNode, QueryNode> queryNodeFactory;

    public FilterAssigner(@Nonnull CQuery query,
                          @Nonnull Function<ProtoQueryNode, QueryNode> queryNodeFactory) {
        this.queryNodeFactory = queryNodeFactory;
        term2filter = HashMultimap.create();
        for (Modifier modifier : query.getModifiers()) {
            if (modifier instanceof SPARQLFilter) {
                SPARQLFilter filter = (SPARQLFilter) modifier;
                filters.add(filter);
                filter.getVarTerms().forEach(t -> term2filter.put(t, filter));
            }
        }
    }

    public FilterAssigner(@Nonnull CQuery query) {
        this(query, p -> new QueryNode(p.getEndpoint(), p.getQuery()));
    }

    public boolean isEmpty() {
        return term2filter.isEmpty();
    }

    /**
     * Tries to place as many {@link SPARQLFilter} as possible in each {@link QueryNode}.
     *
     * Insertions will be registered on the placement map.
     *
     * @param list list of {@link ProtoQueryNode}s, without nulls
     * @return List of {@link QueryNode}s, without nulls
     */
    public @Nonnull List<QueryNode> placeFiltersOnLeaves(@Nonnull List<ProtoQueryNode> list) {
        List<QueryNode> result = new ArrayList<>(list.size());
        for (ProtoQueryNode proto : list) {
            QueryNode queryNode = queryNodeFactory.apply(proto);
            Set<Var> vars = proto.getQuery().getVars();
            vars.stream()
                    .flatMap(v -> term2filter.get(v).stream())
                    .distinct().filter(a -> vars.containsAll(a.getVarTerms()))
                    .forEach(queryNode::addFilter);
            result.add(queryNode);
        }
        return result;
    }

    private boolean canFilter(@Nonnull PlanNode node, @Nonnull SPARQLFilter annotation) {
        return node.getAllVars().containsAll(annotation.getVarTermNames());
    }

    public void placeBottommost(@Nonnull PlanNode plan) {
        for (SPARQLFilter filter : filters) placeBottommost(plan, filter);
    }

    public boolean placeBottommost(@Nonnull PlanNode node,
                                   @Nonnull SPARQLFilter annotation) {
        if (!canFilter(node, annotation))
            return false; // do not recurse into this subtree
        if (node.getFilters().contains(annotation))
            return true; // already added
        if (node instanceof MultiQueryNode) {
            int count = 0;
            for (PlanNode child : node.getChildren())
                count += placeBottommost(child, annotation) ? 1 : 0;
            assert count > 0
                    : "MultiQueryNode as whole accepts "+annotation+" but no child accepts!";
            assert count == node.getChildren().size()
                    : "Not all children of a MultiQueryNode could receive "+annotation;
        } else if (node instanceof QueryNode) {
            node.addFilter(annotation);
        } else {
            boolean done = false;
            for (PlanNode child : node.getChildren())
                done |= placeBottommost(child, annotation);
            if (!done)
                node.addFilter(annotation);
        }
        return true;
    }
}
