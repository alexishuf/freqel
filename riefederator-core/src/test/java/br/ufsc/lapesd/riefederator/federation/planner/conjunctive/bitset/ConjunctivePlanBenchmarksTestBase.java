package br.ufsc.lapesd.riefederator.federation.planner.conjunctive.bitset;

import br.ufsc.lapesd.riefederator.BSBMSelfTest;
import br.ufsc.lapesd.riefederator.LargeRDFBenchSelfTest;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.leaf.QueryOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.federation.SimpleFederationModule;
import br.ufsc.lapesd.riefederator.federation.Source;
import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.FilterAssigner;
import br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlanner;
import br.ufsc.lapesd.riefederator.federation.planner.ConjunctivePlannerTest;
import br.ufsc.lapesd.riefederator.federation.planner.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.PrePlanner;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParserTest;
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
import java.util.List;
import java.util.stream.Collectors;

public abstract class ConjunctivePlanBenchmarksTestBase {
    private Injector defInjector = Guice.createInjector(new SimpleFederationModule());

    private @Nonnull List<Source> largeRDFBenchSources() throws IOException {
        List<Source> list = new ArrayList<>();
        for (String filename : LargeRDFBenchSelfTest.DATA_FILENAMES) {
            ARQEndpoint ep = ARQEndpoint.forModel(LargeRDFBenchSelfTest.loadData(filename));
            list.add(new Source(new SelectDescription(ep), ep));
        }
        return list;
    }

    private @Nonnull List<Source> bsbmSources() throws IOException {
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

        PrePlanner prePlanner = defInjector.getInstance(PrePlanner.class);
        root = prePlanner.plan(root);
        assert root.assertTreeInvariants();
        SPARQLParserTest.assertUniverses(root);

        DecompositionStrategy decomposer = defInjector.getInstance(DecompositionStrategy.class);
        sources.forEach(decomposer::addSource);
        return TreeUtils.streamPreOrder(root).filter(QueryOp.class::isInstance).map(o -> {
            CQuery cQuery = ((QueryOp) o).getQuery();
            List<Op> leaves = new ArrayList<>(decomposer.decomposeIntoLeaves(cQuery));
            for (Op leaf : leaves)
                SPARQLParserTest.assertUniverses(leaf);
            return ImmutablePair.of(cQuery, leaves);
        }).collect(Collectors.toList());
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
        FilterAssigner assigner = new FilterAssigner(query.getModifiers().filters());
        assigner.placeBottommost(plan);
        TreeUtils.copyNonFilter(plan, query.getModifiers());
        ConjunctivePlannerTest.assertPlanAnswers(plan, query);
    }
}
