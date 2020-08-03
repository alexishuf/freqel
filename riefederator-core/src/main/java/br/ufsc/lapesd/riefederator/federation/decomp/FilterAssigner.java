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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Assign {@link SPARQLFilter}s of a query to the deepest possible node in plans.
 */
public class FilterAssigner {
    private static final Logger logger = LoggerFactory.getLogger(FilterAssigner.class);

    private final @Nonnull Set<SPARQLFilter> filters = new HashSet<>();
    private final @Nonnull SetMultimap<Term, SPARQLFilter> term2filter;

    public FilterAssigner(@Nonnull CQuery query) {
        term2filter = HashMultimap.create();
        for (Modifier modifier : query.getModifiers()) {
            if (modifier instanceof SPARQLFilter) {
                SPARQLFilter filter = (SPARQLFilter) modifier;
                filters.add(filter);
                filter.getVarTerms().forEach(t -> term2filter.put(t, filter));
            }
        }
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
     * @return List of {@link QueryNode}s or {@link MultiQueryNode}s, without nulls
     */
    public @Nonnull List<PlanNode> placeFiltersOnLeaves(@Nonnull List<ProtoQueryNode> list) {
        List<PlanNode> result = new ArrayList<>(list.size());
        for (ProtoQueryNode proto : list) {
            PlanNode leafNode = proto.toNode(); //QueryNode or MultiQueryNode
            Set<Var> vars = proto.getMatchedQuery().attr().allVars();
            vars.stream()
                    .flatMap(v -> term2filter.get(v).stream())
                    .distinct().filter(a -> vars.containsAll(a.getVarTerms()))
                    .forEach(f -> addFilter(leafNode, f));
            result.add(leafNode);
        }
        return result;
    }

    private static void addFilter(@Nonnull PlanNode node, @Nonnull SPARQLFilter filter) {
        if (node instanceof QueryNode) {
            node.addFilter(filter);
        } else if (node instanceof MultiQueryNode) {
            for (PlanNode child : node.getChildren()) {
                assert child instanceof QueryNode : "Expected MultiNode of QueryNodes";
                assert canFilter(child, filter)
                        : "Filter has variables which do not occur in the alternative node";
                child.addFilter(filter);
            }
        } else {
            assert false : "Only QueryNodes and MultiQueryNodes should be here!";
            node.addFilter(filter);
        }
    }

    private static boolean canFilter(@Nonnull PlanNode node, @Nonnull SPARQLFilter filter) {
        return node.getAllVars().containsAll(filter.getVarTermNames());
    }

    public void placeBottommost(@Nonnull PlanNode plan) {
        for (SPARQLFilter filter : filters) {
            if (!placeBottommost(plan, filter)) {
                HashSet<String> missing = new HashSet<>(filter.getVarTermNames());
                missing.removeAll(plan.getAllVars());
                SPARQLFilter clean = filter.withVarTermsUnbound(missing);
                if (!placeBottommost(plan, clean)) {
                    logger.warn("{} mentions variables not found in the plan, and after " +
                                "statically evaluating bound() calls on those missing variables " +
                                "the filter {} still could not be placed anywhere on the tree. " +
                                "Will effectively ignore this FILTER.", filter, clean);
                }
            }
        }
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
