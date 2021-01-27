package br.ufsc.lapesd.freqel.federation.decomp.agglutinator;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.inner.UnionOp;
import br.ufsc.lapesd.freqel.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.freqel.description.CQueryMatch;
import br.ufsc.lapesd.freqel.federation.Source;
import br.ufsc.lapesd.freqel.federation.decomp.match.SourcesListMatchingStrategy;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.federation.decomp.agglutinator.AgglutinatorTest.*;
import static org.testng.Assert.assertFalse;

public class StandardAgglutinatorTest {
    @DataProvider public static Object[][] agglutinateBenchmarksData() throws Exception {
        return Arrays.stream(AgglutinatorTest.agglutinateBenchmarksData())
                .filter(r -> r[2] instanceof StandardAgglutinator
                          || r[2] instanceof ParallelStandardAgglutinator)
                .toArray(Object[][]::new);
    }

    @BeforeClass(groups = {"fast"})
    public void beforeClass() {
        new AgglutinatorTest().beforeClass();
    }

    @AfterClass(groups = {"fast"})
    public void afterClass() {
        new AgglutinatorTest().afterClass();
    }

    @Test(dataProvider = "agglutinateBenchmarksData", groups = {"fast"})
    public void testAgglutinateBenchmarks(@Nonnull CQuery query, @Nonnull List<Source> sources,
                                          @Nonnull Agglutinator agglutinator) {
        SourcesListMatchingStrategy matchingStrategy = new SourcesListMatchingStrategy();
        sources.forEach(matchingStrategy::addSource);
        agglutinator.setMatchingStrategy(matchingStrategy);
        AgglutinatorTest.InterceptingAgglutinator intercepting
                = new AgglutinatorTest.InterceptingAgglutinator(agglutinator);
        Collection<Op> nodes = matchingStrategy.match(query, intercepting);
        checkValidAgglutination(nodes, query);
        checkLostComponents(intercepting.matches, nodes);
        checkExclusiveGroups(nodes, intercepting.matches);
    }

    private void checkExclusiveGroups(Collection<Op> groupedNodes,
                                      Map<TPEndpoint, CQueryMatch> matches) {
        List<EndpointQueryOp> nodes = groupedNodes.stream()
                .flatMap(n -> n instanceof UnionOp
                        ? n.getChildren().stream().map(c -> (EndpointQueryOp) c)
                        : Stream.of((EndpointQueryOp) n))
                .collect(Collectors.toList());

        IdentityHashMap<TPEndpoint, List<CQuery>> ep2nodes = new IdentityHashMap<>();
        for (EndpointQueryOp n : nodes)
            ep2nodes.computeIfAbsent(n.getEndpoint(), k -> new ArrayList<>()).add(n.getQuery());

        for (Map.Entry<TPEndpoint, List<CQuery>> e : ep2nodes.entrySet()) {
            for (int i = 0, nQueries = e.getValue().size(); i < nQueries; i++) {
                CQuery outer = e.getValue().get(i);
                TPEndpoint ep = e.getKey();
                if (matches.get(ep).getKnownExclusiveGroups().contains(outer)) continue;
                if (!outer.attr().matchedTriples().equals(outer.attr().getSet())) continue;
                if (!isExclusive(outer, ep2nodes)) continue;
                for (int j = 0; j < nQueries; j++) {
                    if (i == j) continue;
                    CQuery inner = e.getValue().get(j);
                    if (matches.get(ep).getKnownExclusiveGroups().contains(inner)) continue;
                    if (!inner.attr().matchedTriples().equals(inner.attr().getSet())) continue;
                    if (!isExclusive(inner, ep2nodes)) continue;
                    assertFalse(MergeHelper.canMerge(inner, outer),
                            "Two queries are exclusive and could be merged");
                }
            }
        }
    }

    private static boolean isExclusive(@Nonnull CQuery q,
                                       @Nonnull Map<TPEndpoint, List<CQuery>> ep2queries) {
        long count = ep2queries.values().stream()
                .filter(list -> list.stream().anyMatch(other -> isContained(q, other))).count();
        assert count > 0;
        return count == 1;
    }

}