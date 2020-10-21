package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.setMinus;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Assign {@link SPARQLFilter}s of a query to the deepest possible node in plans.
 */
public class FilterAssigner {
    private static final Logger logger = LoggerFactory.getLogger(FilterAssigner.class);

    /**
     * Tries to place as many {@link SPARQLFilter} as possible in each {@link EndpointQueryOp}.
     */
    public static void placeFiltersOnLeaves(@Nonnull Collection<? extends Op> nodes,
                                            @Nonnull Collection<SPARQLFilter> filters) {
        for (Op leafNode : nodes) {
            Set<String> vars = leafNode.getAllVars();
            for (SPARQLFilter filter : filters) {
                if (vars.containsAll(filter.getVarNames())) {
                    if (leafNode instanceof UnionOp) {
                        for (Op child : leafNode.getChildren()) {
                            assert child instanceof EndpointQueryOp
                                    : "Expected UnionOp of EndpointQueryOp";
                            child.modifiers().add(filter);
                        }
                    } else {
                        assert leafNode instanceof EndpointQueryOp : "Unexpected Op class!";
                        leafNode.modifiers().add(filter);
                    }
                }
            }
        }
    }

    private static boolean canFilter(@Nonnull Op node, @Nonnull SPARQLFilter filter) {
        return node.getAllVars().containsAll(filter.getVarNames());
    }

    public static void placeInnerBottommost(@Nonnull Op root,
                                            @Nonnull Collection<SPARQLFilter> filters) {
        for (SPARQLFilter filter : filters) {
            if (!placeInnerBottommost(root, filter)) {
                Set<String> missing = setMinus(filter.getVarNames(), root.getAllVars());
                SPARQLFilter clean = filter.withVarsEvaluatedAsUnbound(missing);
                if (!placeInnerBottommost(root, clean)) {
                    logger.warn("{} mentions variables not found in the plan, and after " +
                                "statically evaluating bound() calls on those missing variables " +
                                "the filter {} still could not be placed anywhere on the tree. " +
                                "Will effectively ignore this FILTER.", filter, clean);
                }
            }
        }
        assert allFiltersPlaced(root, filters);
    }

    private static boolean allFiltersPlaced(@Nonnull Op root,
                                            @Nonnull Collection<SPARQLFilter> filters) {
        Set<SPARQLFilter> observed = TreeUtils.streamPreOrder(root)
                .flatMap(o -> o.modifiers().filters().stream()).collect(toSet());
        List<SPARQLFilter> missing;
        missing = filters.stream().filter(f -> !observed.contains(f)).collect(toList());
        return missing.isEmpty();
    }

    public static boolean placeInnerBottommost(@Nonnull Op node,
                                               @Nonnull SPARQLFilter filter) {
        if (!canFilter(node, filter))
            return false; // do not recurse into this subtree
        if (node.modifiers().filters().contains(filter))
            return true; // already added
        if (node instanceof UnionOp) {
            int count = 0;
            for (Op child : node.getChildren())
                count += placeInnerBottommost(child, filter) ? 1 : 0;
            assert count > 0
                    : "UnionOp as whole accepts "+filter+" but no child accepts!";
            assert count == node.getChildren().size()
                    : "Not all children of a UnionOp could receive "+filter;
        } else if (node instanceof EndpointQueryOp) {
            node.modifiers().add(filter);
        } else {
            boolean done = false;
            for (Op child : node.getChildren())
                done |= placeInnerBottommost(child, filter);
            if (!done)
                node.modifiers().add(filter);
        }
        return true;
    }
}
