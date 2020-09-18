package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.*;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Assign {@link SPARQLFilter}s of a query to the deepest possible node in plans.
 */
public class FilterAssigner {
    private static final Logger logger = LoggerFactory.getLogger(FilterAssigner.class);

    private final @Nonnull Set<SPARQLFilter> filters;
    private final @Nonnull SetMultimap<Term, SPARQLFilter> term2filter;

    public FilterAssigner(@Nonnull Collection<SPARQLFilter> filters) {
        this.term2filter = HashMultimap.create();
        this.filters = filters instanceof Set ? (Set<SPARQLFilter>)filters : new HashSet<>(filters);
        for (SPARQLFilter filter : filters)
            filter.getVars().forEach(t -> term2filter.put(t, filter));
    }

    public boolean isEmpty() {
        return term2filter.isEmpty();
    }

    /**
     * Tries to place as many {@link SPARQLFilter} as possible in each {@link EndpointQueryOp}.
     *
     * Insertions will be registered on the placement map.
     *
     * @param list list of {@link ProtoQueryOp}s, without nulls
     * @return List of {@link EndpointQueryOp}s or {@link UnionOp}s, without nulls
     */
    public @Nonnull List<Op> placeFiltersOnLeaves(@Nonnull List<ProtoQueryOp> list) {
        List<Op> result = new ArrayList<>(list.size());
        for (ProtoQueryOp proto : list) {
            Op leafNode = proto.toOp(); //QueryNode or MultiQueryNode
            Set<Var> vars = proto.getMatchedQuery().attr().allVars();
            vars.stream()
                    .flatMap(v -> term2filter.get(v).stream())
                    .distinct().filter(a -> vars.containsAll(a.getVars()))
                    .forEach(f -> addFilter(leafNode, f));
            result.add(leafNode);
        }
        return result;
    }

    private static void addFilter(@Nonnull Op node, @Nonnull SPARQLFilter filter) {
        if (node instanceof EndpointQueryOp) {
            node.modifiers().add(filter);
        } else if (node instanceof UnionOp) {
            for (Op child : node.getChildren()) {
                assert child instanceof EndpointQueryOp : "Expected MultiNode of QueryNodes";
                assert canFilter(child, filter)
                        : "Filter has variables which do not occur in the alternative node";
                child.modifiers().add(filter);
            }
        } else {
            assert false : "Only QueryNodes and MultiQueryNodes should be here!";
            node.modifiers().add(filter);
        }
    }

    private static boolean canFilter(@Nonnull Op node, @Nonnull SPARQLFilter filter) {
        return node.getAllVars().containsAll(filter.getVarNames());
    }

    public void placeBottommost(@Nonnull Op plan) {
        for (SPARQLFilter filter : filters) {
            if (!placeBottommost(plan, filter)) {
                HashSet<String> missing = new HashSet<>(filter.getVarNames());
                missing.removeAll(plan.getAllVars());
                SPARQLFilter clean = filter.withVarsEvaluatedAsUnbound(missing);
                if (!placeBottommost(plan, clean)) {
                    logger.warn("{} mentions variables not found in the plan, and after " +
                                "statically evaluating bound() calls on those missing variables " +
                                "the filter {} still could not be placed anywhere on the tree. " +
                                "Will effectively ignore this FILTER.", filter, clean);
                }
            }
        }
        assert allFiltersPlaced(plan);
    }

    private boolean allFiltersPlaced(@Nonnull Op root) {
        Set<SPARQLFilter> observed = TreeUtils.streamPreOrder(root)
                .flatMap(o -> o.modifiers().filters().stream()).collect(toSet());
        List<SPARQLFilter> missing = filters.stream().filter(f -> !observed.contains(f)).collect(toList());
        return missing.isEmpty();
    }

    public boolean placeBottommost(@Nonnull Op node,
                                   @Nonnull SPARQLFilter annotation) {
        if (!canFilter(node, annotation))
            return false; // do not recurse into this subtree
        if (node.modifiers().filters().contains(annotation))
            return true; // already added
        if (node instanceof UnionOp) {
            int count = 0;
            for (Op child : node.getChildren())
                count += placeBottommost(child, annotation) ? 1 : 0;
            assert count > 0
                    : "MultiQueryNode as whole accepts "+annotation+" but no child accepts!";
            assert count == node.getChildren().size()
                    : "Not all children of a MultiQueryNode could receive "+annotation;
        } else if (node instanceof EndpointQueryOp) {
            node.modifiers().add(annotation);
        } else {
            boolean done = false;
            for (Op child : node.getChildren())
                done |= placeBottommost(child, annotation);
            if (!done)
                node.modifiers().add(annotation);
        }
        return true;
    }
}
