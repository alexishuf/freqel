package br.ufsc.lapesd.riefederator.federation.planner.conjunctive;

import br.ufsc.lapesd.riefederator.NamedFunction;
import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.JoinInfo;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.priv.BitJoinGraph;
import br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.priv.InputsBitJoinGraph;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.EmptyEndpoint;
import br.ufsc.lapesd.riefederator.util.indexed.ref.RefIndexSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.algebra.JoinInfo.getJoinability;
import static br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset.BitsetConjunctivePlannerTest.withUniverseSets;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation.asRequired;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.*;

public class JoinGraphTest implements TestContext {
    private @Nonnull QueryOp q(Object... args) {
        return new QueryOp(createQuery(args));
    }

    private static final List<NamedFunction<RefIndexSet<Op>, JoinGraph>> factoriesForInputs = asList(
            new NamedFunction<>("InputsBitJoinGraph", i -> new InputsBitJoinGraph(i)),
            new NamedFunction<>("ArrayJoinGraph()", i -> new ArrayJoinGraph(i))
    );

    private static final List<NamedFunction<RefIndexSet<Op>, JoinGraph>> factories = Stream.concat(
            Stream.of(new NamedFunction<RefIndexSet<Op>, JoinGraph>(
                    "BitJoinGraph", i -> new BitJoinGraph(i))),
            factoriesForInputs.stream()
    ).collect(toList());

    @DataProvider public @Nonnull Object[][] pathsData() {
        return Stream.of(1, 2, 4, 8, 16)
                .flatMap(i -> factories.stream().map(f -> new Object[] {f, i}))
                .toArray(Object[][]::new);
    }

    @DataProvider public @Nonnull Object[][] inputsPathsData() {
        return Stream.of(1, 2, 4, 8, 16)
                .flatMap(i -> factoriesForInputs.stream().map(f -> new Object[] {f, i}))
                .toArray(Object[][]::new);
    }

    @Test(dataProvider = "pathsData")
    public void testPath(@Nonnull Function<RefIndexSet<Op>, JoinGraph> fac, int nNodes) {
        List<Op> nodesList = new RefIndexSet<>(nNodes);
        for (int i = 0; i < nNodes; i++)
            nodesList.add(q(new StdVar("x"+i), p1, new StdVar("x"+(i+1))));
        RefIndexSet<Op> nodes = RefIndexSet.fromRefDistinct(withUniverseSets(nodesList));

        JoinGraph graph = fac.apply(nodes);
        assertEquals(graph.size(), nodes.size());
        assertTrue(nodes.stream().allMatch(n -> graph.indexOf(n) >= 0));

        for (int i = 0; i < nNodes; i++) {
            Set<Integer> actual = new HashSet<>(), expected = new HashSet<>();
            if (i   > 0           ) expected.add(i-1);
            if (i+1 < nodes.size()) expected.add(i+1);
            graph.forEachNeighborIndex(i, actual::add);
            assertEquals(actual, expected, "i="+i);
        }
        for (int i = 0; i < nNodes; i++) {
            Set<ImmutablePair<JoinInfo, Op>> actual = new HashSet<>(), expected = new HashSet<>();
            Op node = nodes.get(i);
            if (i > 0) {
                Op prev = nodes.get(i - 1);
                expected.add(ImmutablePair.of(getJoinability(prev, node), prev));
            }
            if (i+1 < nodes.size()) {
                Op next = nodes.get(i + 1);
                expected.add(ImmutablePair.of(getJoinability(node, next), next));
            }
            graph.forEachNeighbor(i, (info, op) -> actual.add(ImmutablePair.of(info, op)));
            assertEquals(actual, expected, "i="+i);
        }
        for (int i = 0; i < nNodes; i++) {
            assertNull(graph.getWeight(i, i));
            for (int j = i+1; j < nNodes; j++) {
                JoinInfo expected = j == i+1 ? getJoinability(nodes.get(i), nodes.get(j)) : null;
                assertEquals(graph.getWeight(i, j), expected);
                assertEquals(graph.getWeight(j, i), expected);
            }
        }
    }

    @Test(dataProvider = "inputsPathsData")
    public void testPathOverInputs(@Nonnull Function<RefIndexSet<Op>, JoinGraph> fac,
                                   int pathLen) {
        List<Op> nodesList = new ArrayList<>();
        List<Atom> atoms = range(0, pathLen).mapToObj(i -> new Atom("A" + i)).collect(toList());
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < pathLen; j++) {
                StdVar s = new StdVar("x" + j), o = new StdVar("x" + (j + 1));
                MutableCQuery q = createQuery(s, new StdURI(EX+"p"+j), o);
                if (i == 0 && j > 0)
                    q.annotate(s, asRequired(atoms.get(i), "subj").get());
                else if (i == 1 && j < pathLen-1)
                    q.annotate(o, asRequired(atoms.get(i), "obj" ).get());
                nodesList.add(new EndpointQueryOp(new EmptyEndpoint(), q));
            }
        }
        RefIndexSet<Op> nodes = RefIndexSet.fromRefDistinct(withUniverseSets(nodesList));

        JoinGraph graph = fac.apply(nodes);
        assertEquals(graph.size(), nodes.size());
        assertTrue(nodes.stream().allMatch(n -> graph.indexOf(n) >= 0));

        for (int i = 0; i < nodes.size(); i++) {
            assertNull(graph.getWeight(i, i));
            for (int j = i+1; j < nodes.size(); j++) {
                JoinInfo expected = getJoinability(nodes.get(i), nodes.get(j));
                expected = expected.isValid() ? expected : null;
                assertEquals(graph.getWeight(i, j), expected);
                assertEquals(graph.getWeight(j, i), expected);
            }
        }
        for (int i = 0; i < nodes.size(); i++) {
            Op left = nodes.get(i);
            Set<Integer> actual = new HashSet<>(), expected = new HashSet<>();
            for (int j = 0; j < nodes.size(); j++) {
                Op right = nodes.get(j);
                if (JoinInfo.getJoinability(left, right).isValid())
                    expected.add(j);
            }
            graph.forEachNeighborIndex(i, actual::add);
            assertEquals(actual, expected, "i="+i);
        }
    }

}