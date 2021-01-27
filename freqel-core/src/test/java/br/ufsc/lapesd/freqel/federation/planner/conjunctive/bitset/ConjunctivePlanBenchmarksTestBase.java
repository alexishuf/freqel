package br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.freqel.BSBMSelfTest;
import br.ufsc.lapesd.freqel.LargeRDFBenchSelfTest;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.federation.SimpleFederationModule;
import br.ufsc.lapesd.freqel.federation.Source;
import br.ufsc.lapesd.freqel.federation.concurrent.PlanningExecutorService;
import br.ufsc.lapesd.freqel.federation.decomp.FilterAssigner;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.Agglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.match.MatchingStrategy;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.ConjunctivePlannerTest;
import br.ufsc.lapesd.freqel.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.PrePlanner;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParserTest;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
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
    private final Injector defInjector = Guice.createInjector(new SimpleFederationModule());

    @Test(enabled = false)
    public static @Nonnull List<Source> largeRDFBenchSources() throws IOException {
        List<Source> list = new ArrayList<>();
        for (String filename : LargeRDFBenchSelfTest.DATA_FILENAMES) {
            ARQEndpoint ep = ARQEndpoint.forModel(LargeRDFBenchSelfTest.loadData(filename));
            list.add(new Source(new SelectDescription(ep), ep));
        }
        return list;
    }

    @Test(enabled = false)
    public static @Nonnull List<Source> bsbmSources() throws IOException {
        ArrayList<Source> list = new ArrayList<>();
        for (String filename : BSBMSelfTest.DATA_FILENAMES) {
            ARQEndpoint ep = ARQEndpoint.forModel(BSBMSelfTest.loadData(filename));
            list.add(new Source(new SelectDescription(ep), ep));
        }
        return list;
    }

    private @Nonnull List<ImmutablePair<CQuery, List<Op>>>
    getQueriesAndFragments(@Nonnull Op query, @Nonnull List<Source> sources) {
        SPARQLParserTest.assertUniverses(query);
        Op root = TreeUtils.deepCopy(query);
        assert root.assertTreeInvariants();
        SPARQLParserTest.assertUniverses(root);

        PlanningExecutorService executor = defInjector.getInstance(PlanningExecutorService.class);
        executor.bind();
        try {

            PrePlanner prePlanner = defInjector.getInstance(PrePlanner.class);
            root = prePlanner.plan(root);
            assert root.assertTreeInvariants();
            SPARQLParserTest.assertUniverses(root);

            MatchingStrategy matchingStrategy = defInjector.getInstance(MatchingStrategy.class);
            Agglutinator agglutinator = defInjector.getInstance(Agglutinator.class);
            agglutinator.setMatchingStrategy(matchingStrategy);
            sources.forEach(matchingStrategy::addSource);
            return TreeUtils.streamPreOrder(root).filter(QueryOp.class::isInstance).map(o -> {
                CQuery cQuery = ((QueryOp) o).getQuery();
                Collection<Op> leaves = matchingStrategy.match(cQuery, agglutinator);
                for (Op leaf : leaves)
                    SPARQLParserTest.assertUniverses(leaf);
                return ImmutablePair.of(cQuery, (List<Op>) leaves);
            }).collect(Collectors.toList());
        } finally {
            executor.release();
        }
    }

    @DataProvider public @Nonnull Object[][] planData() throws IOException, SPARQLParseException {
        List<ImmutablePair<CQuery, List<Op>>> list = new ArrayList<>();
        List<Source> lrbSources = largeRDFBenchSources();
        for (String filename : LargeRDFBenchSelfTest.QUERY_FILENAMES) {
            Op query = LargeRDFBenchSelfTest.loadQuery(filename);
            list.addAll(getQueriesAndFragments(query, lrbSources));
        }
        List<Source> bsbmSources = bsbmSources();
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
        Module module = Modules.override(new SimpleFederationModule()).with(new AbstractModule() {
            @Override protected void configure() {
                super.configure();
                bind(JoinOrderPlanner.class).to(joinOrderPlannerClass);
            }
        });
        ConjunctivePlanner planner = Guice.createInjector(module).getInstance(getPlannerClass());
        Op plan = planner.plan(query, fragments);
        // ConjunctivePlanner is modifier-oblivious. Add them so that assertPlanAnswers() passes.
        FilterAssigner.placeInnerBottommost(plan, query.getModifiers().filters());
        TreeUtils.copyNonFilter(plan, query.getModifiers());
        ConjunctivePlannerTest.assertPlanAnswers(plan, query);
    }
}
