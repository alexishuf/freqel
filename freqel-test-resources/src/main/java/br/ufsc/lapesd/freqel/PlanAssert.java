package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.algebra.JoinInfo;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.CartesianOp;
import br.ufsc.lapesd.freqel.algebra.inner.ConjunctionOp;
import br.ufsc.lapesd.freqel.algebra.inner.JoinOp;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.DQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EmptyOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterNode;
import br.ufsc.lapesd.freqel.util.indexed.FullIndexSet;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.ref.IdentityHashSet;
import br.ufsc.lapesd.freqel.util.ref.RefSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.serializer.SerializationContext;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.isAcyclic;
import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.streamPreOrder;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.*;

public class PlanAssert {
    public static void assertPlanAnswers(@Nonnull Op root, @Nonnull CQuery query) {
        assertPlanAnswers(root, query, false, false);
    }
    public static void assertPlanAnswers(@Nonnull Op root, @Nonnull Op query) {
        assertPlanAnswers(root, query, false, false);
    }
    public static void assertPlanAnswers(@Nonnull Op root, @Nonnull CQuery query,
                                         boolean allowEmptyNode, boolean forgiveFilters) {
        assertPlanAnswers(root, new QueryOp(query), allowEmptyNode, forgiveFilters);
    }
    public static void assertPlanAnswers(@Nonnull Op root, @Nonnull Op query,
                                         boolean allowEmptyNode, boolean forgiveFilters) {
        IndexSet<Triple> triples = FullIndexSet.from(query.getMatchedTriples());

        // the plan is acyclic
        assertTrue(isAcyclic(root));

        if (!allowEmptyNode) {
            assertEquals(root.modifiers().optional(), query.modifiers().optional());
            if (query.modifiers().optional() == null)
                assertFalse(root instanceof EmptyOp, "EmptyOp is not an answer!");
            // tolerate EmptyOp x if x is a child of a union that has a non-EmptyOp child
            RefSet<Op> tolerate = new IdentityHashSet<>();
            streamPreOrder(root).filter(UnionOp.class::isInstance)
                    .forEach(o -> {
                        long c = o.getChildren().stream().filter(EmptyOp.class::isInstance).count();
                        if (c < o.getChildren().size()) {
                            o.getChildren().stream().filter(EmptyOp.class::isInstance)
                                    .forEach(tolerate::add);
                        }
                    });
            // tolerate EmptyOp x if x is marked optional
            streamPreOrder(root)
                    .filter(o -> o instanceof EmptyOp && o.modifiers().optional() != null)
                    .forEach(tolerate::add);
            assertEquals(streamPreOrder(root).filter(EmptyOp.class::isInstance)
                            .filter(o -> !tolerate.contains(o)).count(),
                    0, "There are non-tolerable EmptyOp in the plan as leaves");
        }

        // any query node should only match triples in the query
        List<Op> bad = streamPreOrder(root)
                .filter(n -> n instanceof EndpointQueryOp
                        && !triples.containsAll(n.getMatchedTriples()))
                .collect(toList());
        assertEquals(bad, emptyList());

        // any  node should only match triples in the query
        bad = streamPreOrder(root)
                .filter(n -> !triples.containsAll(n.getMatchedTriples()))
                .collect(toList());
        assertEquals(bad, emptyList());

        // the set of matched triples in the plan must be the same as the query
        assertEquals(root.getMatchedTriples(), triples);

        long queryUnions = dqDeepStreamPreOrder(query).filter(UnionOp.class::isInstance).count();
        if (queryUnions == 0) {
            // all nodes in a MQNode must match the exact same triples in query
            // this allows us to consider the MQNode as a unit in the plan
            bad = dqDeepStreamPreOrder(root).filter(n -> n instanceof UnionOp)
                    .map(n -> (UnionOp) n)
                    .filter(n -> n.getChildren().stream().map(Op::getMatchedTriples)
                            .distinct().count() != 1)
                    .collect(toList());
            assertEquals(bad, emptyList());
        }

        // children of MQ nodes may match the same triples with different triples
        // However, if two children have the same query, then their endpoints must not be
        // equivalent as this would be wasteful. Comparison must use the CQuery instead of
        // Set<Triple> since it may make sense to send the same triples with distinct
        // QueryRelevantTermAnnotations (e.g., WebAPICQEndpoint and JDBCCQEndpoint)
        List<Set<EndpointQueryOp>> equivSets = multiQueryNodes(root).stream()
                .map(n -> {
                    Set<EndpointQueryOp> equiv = new HashSet<>();
                    ListMultimap<CQuery, EndpointQueryOp> mm;
                    mm = MultimapBuilder.hashKeys().arrayListValues().build();
                    for (Op child : n.getChildren()) {
                        if (child instanceof EndpointQueryOp)
                            mm.put(((EndpointQueryOp) child).getQuery(), (EndpointQueryOp) child);
                    }
                    for (CQuery key : mm.keySet()) {
                        for (int i = 0; i < mm.get(key).size(); i++) {
                            EndpointQueryOp outer = mm.get(key).get(i);
                            for (int j = i + 1; j < mm.get(key).size(); j++) {
                                EndpointQueryOp inner = mm.get(key).get(j);
                                if (outer.getEndpoint().isAlternative(inner.getEndpoint()) ||
                                        inner.getEndpoint().isAlternative(outer.getEndpoint())) {
                                    equiv.add(outer);
                                    equiv.add(inner);
                                }
                            }
                        }
                    }
                    return equiv;
                }).filter(s -> !s.isEmpty()).collect(toList());
        assertEquals(equivSets, emptySet());

        // no single-child union  (be it a legit union or a multi-query)
        bad = dqDeepStreamPreOrder(root)
                .filter(n -> n instanceof UnionOp && n.getChildren().size() < 2)
                .collect(toList());
        assertEquals(bad, emptyList());

        // MQ nodes should not be directly nested (that is not elegant)
        bad = multiQueryNodes(root).stream()
                .filter(n -> n.getChildren().stream().anyMatch(n2 -> n2 instanceof UnionOp))
                .collect(toList());
        assertEquals(bad, emptyList());

        // no ConjunctionOp in the plan (should've been replaced with JoinOps)
        bad = dqDeepStreamPreOrder(root).filter(n -> n instanceof ConjunctionOp).collect(toList());
        assertEquals(bad, emptyList());

        // all join nodes are valid joins
        bad = dqDeepStreamPreOrder(root).filter(n -> n instanceof JoinOp).map(n -> (JoinOp) n)
                .filter(n -> !JoinInfo.getJoinability(n.getLeft(), n.getRight()).isValid())
                .collect(toList());
        assertEquals(bad, emptyList());

        // no single-child cartesian nodes
        bad = dqDeepStreamPreOrder(root)
                .filter(n -> n instanceof CartesianOp && n.getChildren().size() < 2)
                .collect(toList());
        assertEquals(bad, emptyList());

        // cartesian nodes should not be directly nested (that is not elegant)
        bad = dqDeepStreamPreOrder(root)
                .filter(n -> n instanceof CartesianOp
                        && n.getChildren().stream()
                        .anyMatch(n2 -> n2 instanceof CartesianOp
                                && n2.modifiers().optional() == null))
                .collect(toList());
        assertEquals(bad, emptyList());

        // no cartesian nodes where a join is applicable between two of its operands
        bad = dqDeepStreamPreOrder(root).filter(n -> n instanceof CartesianOp)
                .filter(n -> {
                    HashSet<Op> children = new HashSet<>(n.getChildren());
                    //noinspection UnstableApiUsage
                    for (Set<Op> pair : Sets.combinations(children, 2)) {
                        Iterator<Op> it = pair.iterator();
                        Op l = it.next();
                        assert it.hasNext();
                        Op r = it.next();
                        if (JoinInfo.getJoinability(l, r).isValid())
                            return true; // found a violation
                    }
                    return false;
                }).collect(toList());

        if (!forgiveFilters) {
            Set<String> allVars = TreeUtils.streamPreOrder(query)
                    .filter(QueryOp.class::isInstance)
                    .flatMap(o -> ((QueryOp)o).getQuery().attr().tripleVarNames().stream())
                    .collect(toSet());
            List<SPARQLFilter> allFilters = streamPreOrder(query)
                    .flatMap(o -> o.modifiers().filters().stream())
                    .map(f -> {
                        if (allVars.containsAll(f.getVarNames()))
                            return f;
                        HashSet<String> missing = new HashSet<>(f.getVarNames());
                        missing.removeAll(allVars);
                        return f.withVarsEvaluatedAsUnbound(missing);
                    }).collect(toList());
            //all filters are placed somewhere
            List<SPARQLFilter> missingFilters = allFilters.stream()
                    .filter(f -> dqDeepStreamPreOrder(root)
                            .noneMatch(n -> n.modifiers().contains(f)))
                    .collect(toList());
            if (missingFilters.isEmpty()) {
                // no extra filters
                List<SPARQLFilter> extraFilters = dqDeepStreamPreOrder(root)
                        .flatMap(n -> n.modifiers().filters().stream())
                        .filter(f -> !allFilters.contains(f)).collect(toList());
                assertEquals(extraFilters, emptyList());
            } else {
                // break up filters into minimal conjunctive components
                Set<SPARQLFilter> allComponents = allFilters.stream()
                        .flatMap(f -> conjunctiveComponents(f).stream()).collect(toSet());
                List<SPARQLFilter> missingComponents = allComponents.stream()
                        .filter(f -> dqDeepStreamPreOrder(root)
                                .noneMatch(n -> n.modifiers().filters().stream()
                                        .anyMatch(af -> conjunctiveComponents(af).contains(f))))
                        .collect(toList());
                assertEquals(missingComponents, emptyList());
            }

            // all filters are placed somewhere valid (with all required vars)
            List<ImmutablePair<? extends Op, SPARQLFilter>> badFilterAssignments;
            badFilterAssignments = dqDeepStreamPreOrder(root)
                    .flatMap(n -> n.modifiers().filters().stream().map(f -> ImmutablePair.of(n, f)))
                    .filter(p -> !p.left.getAllVars().containsAll(p.right.getVarNames()))
                    .collect(toList());
            assertEquals(badFilterAssignments, emptyList());

            // do not place a filter on a node where all of the filter variables are input variables
            List<ImmutablePair<Op, SPARQLFilter>> fullyInputFilters = dqDeepStreamPreOrder(root)
                    .flatMap(n -> n.modifiers().filters().stream().map(f -> ImmutablePair.of(n, f)))
                    .filter(p -> p.left.getInputVars().containsAll(p.right.getVarNames()))
                    .collect(toList());
            assertEquals(fullyInputFilters, emptyList());

            // same as previous, but checks filters within CQuery instances
            badFilterAssignments = dqDeepStreamPreOrder(root)
                    .filter(QueryOp.class::isInstance)
                    .map(n -> (QueryOp)n)
                    .flatMap(n -> n.getQuery().getModifiers().stream()
                            .filter(SPARQLFilter.class::isInstance)
                            .map(f -> ImmutablePair.of(n, (SPARQLFilter)f)))
                    .filter(p -> !p.left.getAllVars().containsAll(p.right.getVarNames()))
                    .collect(toList());
            assertEquals(badFilterAssignments, emptyList());
        }

        assertEquals(bad, emptyList());
    }

    public static @Nonnull Stream<Op> dqDeepStreamPreOrder(@Nonnull Op root) {
        return streamPreOrder(root).flatMap(n -> n instanceof DQueryOp
                ? streamPreOrder(((DQueryOp)n).getQuery()) : Stream.of(n));
    }

    private static final @Nonnull SerializationContext SER_CTX = new SerializationContext();

    public static @Nonnull String getFunctionName(@Nonnull ExprFunction function) {
        String name = function.getOpName();
        if (name == null)
            name = function.getFunctionName(SER_CTX);
        return name;
    }

    private static @Nonnull Set<SPARQLFilter> conjunctiveComponents(@Nonnull SPARQLFilter filter) {
        List<SPARQLFilter> components = new ArrayList<>();
        ArrayDeque<SPARQLFilterNode> stack = new ArrayDeque<>();
        stack.push(filter.getExpr());
        while (!stack.isEmpty()) {
            SPARQLFilterNode expr = stack.pop();
            if (!expr.isTerm()) {
                if (expr.name().equals("&&"))
                    expr.args().forEach(stack::push);
                continue; //expr is a inner node, do not store
            }
            components.add(SPARQLFilterFactory.wrapFilter(expr));
        }

        HashSet<SPARQLFilter> set = new HashSet<>(components);
        assert set.size() == components.size();
        return set;
    }

    private static @Nonnull List<UnionOp> multiQueryNodes(@Nonnull Op root) {
        List<UnionOp> list = new ArrayList<>();
        ArrayDeque<Op> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            Op op = queue.remove();
            if (op instanceof UnionOp) {
                boolean isMQ = op.getChildren().stream().map(Op::getMatchedTriples)
                        .distinct().count() == 1;
                if (isMQ)
                    list.add((UnionOp)op);
            }
            queue.addAll(op.getChildren());
        }
        return list;
    }

}
