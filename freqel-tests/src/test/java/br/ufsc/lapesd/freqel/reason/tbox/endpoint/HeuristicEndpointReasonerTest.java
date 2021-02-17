package br.ufsc.lapesd.freqel.reason.tbox.endpoint;

import br.ufsc.lapesd.freqel.PlanAssert;
import br.ufsc.lapesd.freqel.ResultsAssert;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.description.semantic.SemanticAskDescription;
import br.ufsc.lapesd.freqel.description.semantic.SemanticSelectDescription;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.SimpleFederationModule;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.Reasoning;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.CollectionResults;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.reason.tbox.*;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.jena.rdf.model.Model;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactory.parseFilter;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.freqel.query.results.impl.CollectionResults.greedy;
import static br.ufsc.lapesd.freqel.query.results.impl.CollectionResults.wrapSameVars;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.testng.Assert.assertTrue;

public class HeuristicEndpointReasonerTest implements TestContext {
    private static final @Nonnull StdURI D = new StdURI(EX+"D");
    private static final @Nonnull StdURI D1 = new StdURI(EX+"D1");
    private static final @Nonnull StdURI D11 = new StdURI(EX+"D11");
    private static final @Nonnull StdURI D111 = new StdURI(EX+"D111");
    private static final @Nonnull StdURI D1111 = new StdURI(EX+"D1111");
    private static final @Nonnull StdURI D12 = new StdURI(EX+"D12");
    private static final @Nonnull StdURI D2 = new StdURI(EX+"D2");

    private static final @Nonnull StdURI p = new StdURI(EX+"p");
    private static final @Nonnull StdURI p1 = new StdURI(EX+"p1");
    private static final @Nonnull StdURI p11 = new StdURI(EX+"p11");
    private static final @Nonnull StdURI p111 = new StdURI(EX+"p111");
    private static final @Nonnull StdURI p1111 = new StdURI(EX+"p1111");
    private static final @Nonnull StdURI p12 = new StdURI(EX+"p12");
    private static final @Nonnull StdURI p2 = new StdURI(EX+"p2");


    @DataProvider public @Nonnull Object[][] epReasonerData() {
        List<Module> modules = asList(
                new SimpleFederationModule() {
                    @Override public @Nonnull String toString() {
                        return "default SimpleFederationModule";
                    }
                },
                new SimpleFederationModule() {
                    @Override protected void configure() {
                        super.configure();
                        bind(EndpointReasoner.class).to(HeuristicEndpointReasoner.class);
                    }
                    @Override public @Nonnull String toString() {
                        return "SimpleFederationModule+HeuristicEndpointReasoner";
                    }
                });
        List<BiFunction<TPEndpoint, TBox, ? extends Description>> descriptionFactories = asList(
                (ep, tBox) -> new SemanticSelectDescription((CQEndpoint) ep, tBox),
                SemanticAskDescription::new
        );
        return Lists.cartesianProduct(modules, descriptionFactories).stream().map(List::toArray)
                    .toArray(Object[][]::new);
    }

    @DataProvider public @Nonnull Object[][] queryData() {
        List<List<Object>> base = Arrays.stream(epReasonerData()).map(Arrays::asList)
                                        .collect(Collectors.toList());
        List<List<Object>> queries = asList(
                // queries that require no reasoning and include no reasoning in results
                asList(createQuery(x, type, D2), singleton(MapSolution.build(x, Charlie))),
                asList(createQuery(x, p11, Eric), singleton(MapSolution.build(x, Alice))),
                asList(createQuery(x, type, D11), singleton(MapSolution.build(x, Dave))),
                asList(createQuery(x, type, D11, x, age, integer(23)),
                       singleton(MapSolution.build(x, Dave))),
                asList(createQuery(Alice, age, y), emptySet()),
                asList(createQuery(Dave, age, y), singleton(MapSolution.build(y, integer(23)))),
                asList(createQuery(x, age, y, parseFilter("?y < 25")),
                       singleton(MapSolution.build(x, Dave, y, integer(23)))),

                //subclass inference
                asList(createQuery(x, type, D1, Reasoning.INSTANCE),
                       singleton(MapSolution.build(x, Dave))),
                asList(createQuery(x, type, D1), // do not reason without modifier
                       emptySet()),
                asList(createQuery(x, type, D, Reasoning.INSTANCE),
                       newHashSet(MapSolution.build(x, Dave),
                                  MapSolution.build(x, Charlie),
                                  MapSolution.build(x, Bob))),
                asList(createQuery(x, p1, Eric, Reasoning.INSTANCE),
                       singleton(MapSolution.build(x, Alice)))
        );
        List<List<Object>> rows = new ArrayList<>();
        for (List<List<Object>> lists : Lists.cartesianProduct(base, queries)) {
            ArrayList<Object> row = new ArrayList<>(lists.get(0));
            row.addAll(lists.get(1));
            rows.add(row);
        }
        return rows.stream().map(List::toArray).toArray(Object[][]::new);
    }

    private @Nonnull TBox loadTBox(@Nonnull String ontoPath) {
        TBoxSpec ontoSpec = new TBoxSpec().addResource(getClass(), ontoPath);
        TransitiveClosureTBoxMaterializer tBox = new TransitiveClosureTBoxMaterializer(ontoSpec);
        tBox.setName(ontoPath);
        return tBox;
    }

    private @Nonnull ARQEndpoint
    createEndpoint(@Nonnull String dataPath, @Nonnull TBox tBox,
                   @Nonnull BiFunction<TPEndpoint, TBox, Description> descriptionFactory) {
        Model data = new TBoxSpec().addResource(getClass(), dataPath).loadModel();
        ARQEndpoint ep = ARQEndpoint.forModel(data, dataPath);
        ep.setDescription(descriptionFactory.apply(ep, tBox));
        return ep;
    }

    @Test(dataProvider = "queryData")
    public void testQuery(@Nonnull Module module0,
                          @Nonnull BiFunction<TPEndpoint, TBox, Description> descriptionFactory,
                          @Nonnull Object queryObject,
                          @Nonnull Collection<Solution> expected) throws SPARQLParseException {
        String prefix = "../replacements/generators/subterm-";
        Injector injector0 = Guice.createInjector(module0);
        EndpointReasoner epReasoner = injector0.getInstance(EndpointReasoner.class);
        TBox tBox = loadTBox(prefix + "onto.ttl");
        epReasoner.offerTBox(tBox);
        Module module = Modules.override(module0).with(new AbstractModule() {
            @Override protected void configure() {
                bind(EndpointReasoner.class).toInstance(epReasoner);
            }
        });
        Injector injector = Guice.createInjector(module);
        Federation federation = injector.getInstance(Federation.class);

        ARQEndpoint ep = createEndpoint(prefix + "1.ttl", tBox, descriptionFactory);
        federation.addSource(ep);

        Boolean reasonModifier = null;
        Op query;
        if (queryObject instanceof String) {
            query = SPARQLParser.tolerant().parse(queryObject.toString());
        } else if (queryObject instanceof CQuery) {
            reasonModifier = ((CQuery)queryObject).getModifiers().contains(Reasoning.INSTANCE);
            query = new QueryOp((CQuery) queryObject);
        } else {
            query = (Op)queryObject;
        }
        if (!(epReasoner instanceof NoEndpointReasoner) || query.modifiers().reasoning() == null) {
            Op plan = federation.plan(query);
            PlanAssert.assertPlanAnswers(plan, query, expected.isEmpty(), false);
            CollectionResults actual = greedy(federation.execute(plan));
            ResultsAssert.assertExpectedResults(actual, expected);
        }
        if (Boolean.TRUE.equals(reasonModifier)) {
            MutableCQuery copy = new MutableCQuery(((QueryOp) query).getQuery());
            assertTrue(copy.mutateModifiers().removeIf(Reasoning.class::isInstance));
            QueryOp noReasonQuery = new QueryOp(copy);
            Collection<? extends Solution> subset =
                    greedy(federation.query(noReasonQuery)).getCollection();
            ResultsAssert.assertContainsResults(wrapSameVars(expected), subset);
        } else if (Boolean.FALSE.equals(reasonModifier)) {
            MutableCQuery copy = new MutableCQuery(((QueryOp) query).getQuery());
            assertTrue(copy.mutateModifiers().add(Reasoning.INSTANCE));
            QueryOp reasonQuery = new QueryOp(copy);
            Results superset = greedy(federation.query(reasonQuery));
            ResultsAssert.assertContainsResults(superset, expected);
        }
    }

}