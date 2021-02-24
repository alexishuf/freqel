package br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.freqel.BSBMSelfTest;
import br.ufsc.lapesd.freqel.LargeRDFBenchSelfTest;
import br.ufsc.lapesd.freqel.PlanAssert;
import br.ufsc.lapesd.freqel.SPARQLAssert;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.federation.FreqelConfig;
import br.ufsc.lapesd.freqel.federation.concurrent.PlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.decomp.FilterAssigner;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.match.MatchingStrategy;
import br.ufsc.lapesd.freqel.federation.inject.dagger.DaggerTestComponent;
import br.ufsc.lapesd.freqel.federation.inject.dagger.TestComponent;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlannerTest;
import br.ufsc.lapesd.freqel.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.PrePlanner;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public abstract class ConjunctivePlanBenchmarksTestBase {
    @Test(enabled = false)
    public static @Nonnull List<TPEndpoint> largeRDFBenchSources() throws IOException {
        List<TPEndpoint> list = new ArrayList<>();
        for (String filename : LargeRDFBenchSelfTest.DATA_FILENAMES) {
            ARQEndpoint ep = ARQEndpoint.forModel(LargeRDFBenchSelfTest.loadData(filename));
            list.add(ep.setDescription(new SelectDescription(ep)));
        }
        return list;
    }

    @Test(enabled = false)
    public static @Nonnull List<TPEndpoint> bsbmSources() throws IOException {
        ArrayList<TPEndpoint> list = new ArrayList<>();
        for (String filename : BSBMSelfTest.DATA_FILENAMES) {
            ARQEndpoint ep = ARQEndpoint.forModel(BSBMSelfTest.loadData(filename));
            list.add(ep.setDescription(new SelectDescription(ep)));
        }
        return list;
    }

    private @Nonnull List<ImmutablePair<CQuery, List<Op>>>
    getQueriesAndFragments(@Nonnull Op query, @Nonnull List<TPEndpoint> sources) {
        SPARQLAssert.assertUniverses(query);
        Op root = TreeUtils.deepCopy(query);
        assert root.assertTreeInvariants();
        SPARQLAssert.assertUniverses(root);

        TestComponent testComponent = DaggerTestComponent.builder().build();

        PlanningExecutorService executor = testComponent.planningExecutorService();
        executor.bind();
        try {

            PrePlanner prePlanner = testComponent.prePlanner();
            root = prePlanner.plan(root);
            assert root.assertTreeInvariants();
            SPARQLAssert.assertUniverses(root);

            MatchingStrategy matchingStrategy = testComponent.matchingStrategy();
            Agglutinator agglutinator = testComponent.agglutinator();
            agglutinator.setMatchingStrategy(matchingStrategy);
            sources.forEach(matchingStrategy::addSource);
            return TreeUtils.streamPreOrder(root).filter(QueryOp.class::isInstance).map(o -> {
                CQuery cQuery = ((QueryOp) o).getQuery();
                Collection<Op> leaves = matchingStrategy.match(cQuery, agglutinator);
                for (Op leaf : leaves)
                    SPARQLAssert.assertUniverses(leaf);
                return ImmutablePair.of(cQuery, (List<Op>) leaves);
            }).collect(Collectors.toList());
        } finally {
            executor.release();
        }
    }

    @DataProvider public @Nonnull Object[][] planData() throws IOException, SPARQLParseException {
        List<ImmutablePair<CQuery, List<Op>>> list = new ArrayList<>();
        List<TPEndpoint> lrbSources = largeRDFBenchSources();
        for (String filename : LargeRDFBenchSelfTest.QUERY_FILENAMES) {
            Op query = LargeRDFBenchSelfTest.loadQuery(filename);
            list.addAll(getQueriesAndFragments(query, lrbSources));
        }
        List<TPEndpoint> bsbmSources = bsbmSources();
        for (String filename : BSBMSelfTest.QUERY_FILENAMES) {
            Op query = BSBMSelfTest.loadQuery(filename);
            list.addAll(getQueriesAndFragments(query, bsbmSources));
        }

        return list.stream()
                .flatMap(p -> ConjunctivePlannerTest.joinOrderPlannerClasses.stream()
                        .map(jo -> new Object[] {jo, p.left, p.right}))
                .toArray(Object[][]::new);
    }

    protected abstract @Nonnull Class<? extends ConjunctivePlanner> getPlannerClass();

    /** All BSBM and LRB queries can be answered by this planner */
    @Test(dataProvider = "planData", groups = {"fast"})
    public void testPlan(@Nonnull Class<? extends JoinOrderPlanner> joinOrderPlannerClass,
                         @Nonnull CQuery query, @Nonnull List<Op> fragments) {
        TestComponent.Builder builder = DaggerTestComponent.builder();
        builder.overrideFreqelConfig(FreqelConfig.createDefault()
                .set(FreqelConfig.Key.JOIN_ORDER_PLANNER, joinOrderPlannerClass));
        ConjunctivePlanner planner = builder.build().conjunctivePlanner();
        Op plan = planner.plan(query, fragments);
        // ConjunctivePlanner is modifier-oblivious. Add them so that assertPlanAnswers() passes.
        FilterAssigner.placeInnerBottommost(plan, query.getModifiers().filters());
        TreeUtils.copyNonFilter(plan, query.getModifiers());
        PlanAssert.assertPlanAnswers(plan, query);
    }
}
