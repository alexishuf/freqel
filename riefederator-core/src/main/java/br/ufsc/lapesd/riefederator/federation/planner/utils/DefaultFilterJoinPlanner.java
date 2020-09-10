package br.ufsc.lapesd.riefederator.federation.planner.utils;

import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.JoinInfo;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.PipeOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityComparator;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.jena.ExprUtils;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.endpoint.Capability;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.util.RefEquals;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableBiMap;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.serializer.SerializationContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.*;
import java.util.function.Predicate;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.intersect;
import static java.util.stream.Collectors.toList;

public class DefaultFilterJoinPlanner implements FilterJoinPlanner {
    private final @Nonnull CardinalityComparator cardinalityComparator;
    private final @Nonnull JoinOrderPlanner joinOrderPlanner;

    @Inject
    public DefaultFilterJoinPlanner(@Nonnull CardinalityComparator cardinalityComparator,
                                    @Nonnull JoinOrderPlanner joinOrderPlanner) {
        this.cardinalityComparator = cardinalityComparator;
        this.joinOrderPlanner = joinOrderPlanner;
    }

    @Override
    public @Nonnull Op rewrite(@Nonnull CartesianOp op, @Nonnull Set<RefEquals<Op>> shared) {
        Set<SPARQLFilter> filters = op.modifiers().filters();
        if (filters.isEmpty())
            return op;
        State state = new State(op.getChildren(), shared);
        for (SPARQLFilter filter : filters)
            state.addComponents(filter);
        if (state.hasJoinComponents) {
            op.detachChildren();
            Op root = state.rewriteCartesian(op);
            assert validFilterPlacement(root);
            return root;
        }
        return op;
    }

    @Override
    public @Nonnull Op rewrite(@Nonnull JoinOp op, @Nonnull Set<RefEquals<Op>> shared) {
        Set<SPARQLFilter> filters = op.modifiers().filters();
        if (filters.isEmpty())
            return op;
        State state = new State(op, shared);
        for (SPARQLFilter filter : filters)
            state.addComponents(filter);
        if (state.hasJoinComponents)
            state.rewriteJoin(op);
        return op;
    }

    @Override
    public @Nonnull String toString() {
        return String.format("DefaultFilterJoinPlanner(%s, %s)",
                joinOrderPlanner.toString(), cardinalityComparator.toString());
    }

    private static boolean validFilterPlacement(@Nonnull Op root) {
        for (Iterator<Op> it = TreeUtils.iteratePreOrder(root); it.hasNext(); ) {
            Op op = it.next();
            Set<String> all = op.getAllVars();
            Set<String> ins = op.getInputVars();
            List<SPARQLFilter> bad = op.modifiers().filters().stream()
                    .filter(f -> !all.containsAll(f.getVarTermNames())).collect(toList());
            if (!bad.isEmpty())
                return false;
            bad = op.modifiers().filters().stream()
                    .filter(f -> ins.containsAll(f.getVarTermNames())).collect(toList());
            if (!bad.isEmpty())
                return false;
        }
        return true;
    }

    @VisibleForTesting
    @Nonnull State createState(@Nonnull List<Op> nodes, @Nonnull Set<RefEquals<Op>> shared) {
        return new State(nodes, shared);
    }

    @VisibleForTesting
    class State {
        private final  @Nonnull List<Op> nodes;
        private @Nonnull List<SPARQLFilter> components = new ArrayList<>();
        private @Nonnull List<BitSet> component2node;
        private @Nonnull BitSet orphanComps = new BitSet();
        private @Nonnull Set<RefEquals<Op>> shared;
        private boolean hasJoinComponents = false;

        public State(@Nonnull List<Op> nodes, @Nonnull Set<RefEquals<Op>> shared) {
            this.shared = shared;
            this.nodes = new ArrayList<>(nodes);
            this.nodes.sort(Comparator.comparing(Op::getCardinality, cardinalityComparator));
            this.component2node = new ArrayList<>();
        }

        public State(@Nonnull JoinOp op, @Nonnull Set<RefEquals<Op>> shared) {
            this.shared = shared;
            this.nodes = op.getChildren();
            int size = nodes.size();
            this.component2node = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                component2node.add(new BitSet());
        }

        @VisibleForTesting
        @Nonnull List<SPARQLFilter> getComponents() {
            return components;
        }

        public void addComponents(@Nonnull SPARQLFilter filter) {
            addComponents(filter, filter.getExpr());
        }

        public @Nonnull Op rewriteCartesian(@Nonnull CartesianOp parent) {
            BitSet node2component = assignFilters();
            assert !node2component.isEmpty();
            pushDownFilters(node2component);
            Op root = StepUtils.planConjunction(nodes, joinOrderPlanner);
            addOrphans(root);
            TreeUtils.copyNonFilter(root, parent.modifiers());
            return root;
        }

        public void addOrphans(@Nonnull Op root) {
            for (int i = orphanComps.nextSetBit(0); i >= 0; i = orphanComps.nextSetBit(i + 1))
                root.modifiers().add(components.get(i));
        }

        private void pushDownFilters(@Nonnull BitSet nodesFilters) {
            int nComps = components.size();
            BitSet needsPurge = new BitSet(nodes.size());
            for (int i = nodesFilters.nextSetBit(0); i >=0 ; i = nodesFilters.nextSetBit(i+1)) {
                int compIdx = i % nComps;
                int nodeIdx = (i - compIdx) / nComps;
                needsPurge.set(nodeIdx);
                nodes.set(nodeIdx, pushDownFilter(components.get(compIdx), nodes.get(nodeIdx)));
            }
            for (int i = needsPurge.nextSetBit(0); i >= 0; i = needsPurge.nextSetBit(i+1))
                nodes.get(i).purgeCaches();
        }

        private @Nonnull Op pushDownFilter(@Nonnull SPARQLFilter filter, @Nonnull Op op) {
            Set<String> vars = intersect(op.getAllVars(), filter.getVarTermNames());
            assert !vars.isEmpty();
            Op replacement = pushDownFilter(filter, vars, op);
            if (replacement == null) {
                assert false : "Could not place filter anywhere!";
                return op;
            }
            return replacement;
        }

        private @Nullable Op pushDownFilter(@Nonnull SPARQLFilter filter, @Nonnull Set<String> vars,
                                            @Nonnull Op op) {
            if (!op.getAllVars().containsAll(vars))
                return null; // cannot place here, backtrack
            boolean placed = false;
            if (op instanceof InnerOp && !(op instanceof PipeOp)) {
                try (TakenChildren children = ((InnerOp) op).takeChildren()) {
                    for (int i = 0, size = children.size(); i < size; i++) {
                        Op replacement = pushDownFilter(filter, vars, children.get(i));
                        if (replacement != null) {
                            placed = true;
                            children.set(i, replacement);
                        }
                    }
                }
            }
            if (!placed) {
                if (shared.contains(RefEquals.of(op)))
                    op = new PipeOp(op);
                op.modifiers().add(filter);
            }
            return op;
        }

        public void rewriteJoin(@Nonnull JoinOp parent) {
            assert nodes.size() == 2;
            Op l = nodes.get(0), r = nodes.get(1);
            int receiverIndex;
            BitSet nodesFilters;
            if ((!l.hasInputs() && r.hasInputs()) || r.hasRequiredInputs()) {
                nodesFilters = assignFilters(receiverIndex = 1);
            } else if ((!r.hasInputs() && l.hasInputs()) || l.hasRequiredInputs()) {
                nodesFilters = assignFilters(receiverIndex = 0);
            } else {
                BitSet[] fs = {assignFilters(0), assignFilters(1)};
                receiverIndex = fs[0].cardinality() > fs[1].cardinality() ? 0 : 1;
                nodesFilters = fs[receiverIndex];
            }
            if (nodesFilters.isEmpty())
                return; // no assignment, no change
            if (!yieldsValidJoin(receiverIndex, nodesFilters))
                return; // no change: pushing filters would ruin the join
            parent.modifiers().removeIf(SPARQLFilter.class::isInstance);
            parent.detachChildren();
            pushDownFilters(nodesFilters);
            parent.setChildren(nodes); //updates the internal JoinInfo
            addOrphans(parent);
        }

        boolean yieldsValidJoin(int receiver, @Nonnull BitSet filters) {
            int nComps = components.size();
            int b = nComps*receiver, e = nComps*receiver + nComps;
            Op copy = nodes.get(receiver).flatCopy();
            for (int i = filters.nextSetBit(b); i >= 0 && i < e; i = filters.nextSetBit(i+1))
                copy.modifiers().add(components.get(i-b));
            copy.purgeCachesShallow(); //expose new required inputs
            Op l = receiver == 0 ? copy : nodes.get(0), r = receiver == 1 ? copy : nodes.get(1);
            return JoinInfo.getJoinability(l, r).isValid();
        }

        /**
         * For each component in components, map it to a node in nodes.
         *
         * @return The filter assignment matrix in row-major format. If
         * C = components.size(), and N = nodes.size(), the i*C + j bit of the bitset
         * indicates whether the j-th component is mapped to the i-th node.
         */
        private @Nonnull BitSet assignFilters() {
            int nComp = component2node.size();
            int nNodes = nodes.size();
            assert nComp == components.size();
            BitSet nodeFilters = new BitSet(nNodes*nComp), modifiedNodes = new BitSet(nNodes);
            for (int compIndex = 0; compIndex < nComp; compIndex++) {
                BitSet subset = component2node.get(compIndex);
                int nodeIndex = subset == null ? -1
                                               : bestNodeIndex(this.nodes, subset, modifiedNodes);
                if (nodeIndex < 0) {
                    orphanComps.set(compIndex);
                } else {
                    nodeFilters.set(nodeIndex*nComp + compIndex);
                    modifiedNodes.set(nodeIndex);
                }
            }
            return nodeFilters;
        }

        /**
         * Same as {@link State#assignFilters()}, but only maps to a given node. However, the
         * returned bitset layout accounts for all nodes.
         */
        private @Nonnull BitSet assignFilters(int nodeIndex) {
            int nComp = components.size();
            int begin = nodeIndex * nComp;
            BitSet nodesFilters = new BitSet(nodes.size() * nComp);
            for (int i = 0, component2nodeSize = component2node.size(); i < component2nodeSize; i++) {
                BitSet bs = component2node.get(i);
                if (bs != null && bs.get(nodeIndex))
                    nodesFilters.set(begin+i);
            }
            return nodesFilters;
        }

        private void addComponents(@Nonnull SPARQLFilter filter, @Nonnull Expr expr) {
            if (expr instanceof ExprFunction) {
                ExprFunction function = (ExprFunction) expr;
                String name = function.getOpName();
                if (name == null) name = function.getFunctionName(new SerializationContext());

                if (name.equals("&&")) {
                    for (int i = 1; i <= function.numArgs(); i++)
                        addComponents(filter, function.getArg(i));
                    return;
                }
            }
            Set<Var> exprVars = expr.getVarsMentioned();
            ImmutableBiMap<String, Term> outer = filter.getVar2Term();
            SPARQLFilter.Builder b = SPARQLFilter.builder(expr);
            for (Var v : exprVars)
                b.map(v.getVarName(), outer.get(v.getVarName()));
            addComponent(b.build());
        }

        private void addComponent(SPARQLFilter filter) {
            assert components.stream().noneMatch(filter::equals);
            components.add(filter);
            component2node.add(null);
            int index = components.size() - 1;
            for (int i = 0, size = nodes.size(); i < size; i++) {
                if (hasJoinByComponent(nodes.get(i).getAllVars(), filter.getExpr())) {
                    hasJoinComponents = true;
                    component2node.set(index, new BitSet());
                    component2node.get(index).set(i);
                }
            }
        }
    }

    /**
     * A filter component is a join component for a node n with vars V iff:
     *   1. For every boolean yielding expr reaching from the root expression,
     *      that expr includes at least one var in V;
     *   2. For at least one such boolean yielding expr reaching from the root expression,
     *      that expression involves at least one var not in V.
     *
     * Condition (1) ensures that bind joins will never yield trivial sub expressions.
     * Although trivial sub expressions are not an issue, their existence means that there was
     * a better candidate to receive the expression. Condition (2) ensures joinability: i.e.,
     * the filter component receives a value from some other node that is also a child of
     * the cartesian product
     *
     * @param nodeVars the set V mentioned above ({@link Op#getAllVars()}
     * @param expr the component {@link Expr}
     * @return whether conditions (1) and (2) are both satisfied
     */
    @VisibleForTesting
    static boolean hasJoinByComponent(@Nonnull Set<String> nodeVars, @Nonnull Expr expr) {
        if (expr instanceof ExprFunction) {
            return allHaveNodeVar(nodeVars, expr) && someHasExternalVar(nodeVars, expr);
        }
        return false;
    }

    private static boolean allHaveNodeVar(@Nonnull Set<String> nodeVars, @Nonnull Expr expr) {
        if (expr instanceof ExprFunction) {
            ExprFunction function = (ExprFunction) expr;
            if (ExprUtils.L_OPS.contains(ExprUtils.getFunctionName(function))) {
                for (int i = 1, size = function.numArgs(); i <= size; i++) {
                    if (!allHaveNodeVar(nodeVars, function.getArg(i)))
                        return false;
                }
                return true;
            }
        }
        for (Var v : expr.getVarsMentioned()) {
            if (nodeVars.contains(v.getVarName()))
                return true;
        }
        return false;
    }

    private static boolean someHasExternalVar(@Nonnull Set<String> nodeVars, @Nonnull Expr expr) {
        if (expr instanceof ExprFunction) {
            ExprFunction function = (ExprFunction) expr;
            if (ExprUtils.L_OPS.contains(ExprUtils.getFunctionName(function))) {
                for (int i = 1, size = function.numArgs(); i <= size; i++) {
                    if (someHasExternalVar(nodeVars, function.getArg(i)))
                        return true;
                }
                return false;
            }
        }
        for (Var v : expr.getVarsMentioned()) {
            if (!nodeVars.contains(v.getVarName()))
                return true;
        }
        return false;
    }

    @VisibleForTesting
    static int bestNodeIndex(@Nonnull List<Op> nodes, @Nonnull BitSet subset,
                             @Nonnull BitSet alreadyModified) {
        int nCount = nodes.size(), index;
        index = lastIndex(subset, nCount, i -> alreadyModified.get(i));
        if (index >= 0) return index;

        index = lastIndex(subset, nCount, i -> {
            Op op = nodes.get(i);
            return canFilter(op) && op.hasRequiredInputs();
        });
        if (index >= 0) return index;

        index = lastIndex(subset, nCount, i -> nodes.get(i).hasRequiredInputs());
        if (index >= 0) return index;

        index = lastIndex(subset, nCount, i -> {
            Op op = nodes.get(i);
            return canFilter(op) && op.hasInputs();
        });
        if (index >= 0) return index;

        index = lastIndex(subset, nCount, i -> nodes.get(i).hasInputs());
        if (index >= 0) return index;

        return subset.previousSetBit(nCount-1);
    }

    private static boolean canFilter(@Nonnull Op op) {
        TPEndpoint ep = TreeUtils.getEndpoint(op);
        return ep != null && ep.hasRemoteCapability(Capability.SPARQL_FILTER);
    }

    private static int lastIndex(@Nonnull BitSet subset, int subsetSize,
                                 @Nonnull Predicate<Integer> predicate) {
        for (int i = subsetSize; (i = subset.previousSetBit(i-1)) >= 0; ) {
            if (predicate.test(i)) return i;
        }
        return -1;
    }

}
