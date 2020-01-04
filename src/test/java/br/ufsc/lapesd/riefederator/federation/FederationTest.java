package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.description.AskDescription;
import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.EvenDecomposer;
import br.ufsc.lapesd.riefederator.federation.execution.tree.JoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.FixedHashJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.HashJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.ParallelInMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.impl.HeuristicPlanner;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.XSD;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class FederationTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI DAVE = new StdURI("http://example.org/Dave");
    public static final @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI age = new StdURI(FOAF.age.getURI());
    public static final @Nonnull StdURI author = new StdURI("http://example.org/author");
    public static final @Nonnull StdURI name = new StdURI("http://example.org/name");
    public static final @Nonnull StdURI title = new StdURI("http://example.org/title");
    public static final @Nonnull StdURI genre = new StdURI("http://example.org/genre");
    public static final @Nonnull StdURI genreName = new StdURI("http://example.org/genreName");
    public static final @Nonnull StdLit title1 = StdLit.fromUnescaped("title 1", "en");
    public static final @Nonnull StdLit author1 = StdLit.fromUnescaped("author1", "en");
    public static final @Nonnull StdLit genre1 = StdLit.fromUnescaped("genre1", "en");
    public static final @Nonnull StdLit i23 = StdLit.fromUnescaped("23", new StdURI(XSD.xint.getURI()));
    public static final @Nonnull StdLit i25 = StdLit.fromUnescaped("25", new StdURI(XSD.xint.getURI()));
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");
    public static final @Nonnull StdVar Z = new StdVar("z");

    private static @Nonnull StdURI ex(@Nonnull String local) {
        return new StdURI("http://example.org/"+local);
    }

    private static  @Nonnull ARQEndpoint createEndpoint(@Nonnull String filename) {
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, FederationTest.class.getResourceAsStream("../"+filename), Lang.TTL);
        return ARQEndpoint.forModel(m);
    }

    private static final class SetupSingleEp implements Consumer<Federation> {
        @Override
        public void accept(@Nonnull Federation federation) {
            ARQEndpoint ep = createEndpoint("rdf-1.nt");
            federation.addSource(new Source(new SelectDescription(ep), ep));
        }
        @Override
        public String toString() { return "SetupSingleEp"; }
    }

    private static final class SetupTwoEps implements Consumer<Federation> {
        @Override
        public void accept(@Nonnull Federation federation) {
            ARQEndpoint ep = createEndpoint("rdf-1.nt");
            federation.addSource(new Source(new SelectDescription(ep), ep));
            ep = createEndpoint("rdf-2.nt");
            federation.addSource(new Source(new AskDescription(ep), ep));
        }
        @Override
        public String toString() { return "SetupTwoEps";}
    }

    private static final class SetupBookShop implements Consumer<Federation> {
        @Override
        public void accept(Federation federation) {
            ARQEndpoint authors = createEndpoint("authors.nt");
            federation.addSource(new Source(new SelectDescription(authors), authors));
            ARQEndpoint books = createEndpoint("books.nt");
            federation.addSource(new Source(new SelectDescription(books), books));
            ARQEndpoint genres = createEndpoint("genres.nt");
            federation.addSource(new Source(new SelectDescription(genres), genres));
        }
        @Override
        public String toString() { return "SetupBookShop"; }
    }

    private static final @Nonnull List<Module> moduleList = asList(
            new AbstractModule() {
                @Override
                protected void configure() {
                    bind(Planner.class).to(HeuristicPlanner.class);
                    bind(DecompositionStrategy.class).to(EvenDecomposer.class);
                }
                @Override
                public String toString() { return "HeuristicPlanner+EvenDecomposer"; }
            },
            new AbstractModule() {
                @Override
                protected void configure() {
                    bind(JoinNodeExecutor.class).to(FixedHashJoinNodeExecutor.class);
                    bind(HashJoinResultsFactory.class).toInstance(InMemoryHashJoinResults::new);
                    bind(Planner.class).to(HeuristicPlanner.class);
                    bind(DecompositionStrategy.class).to(EvenDecomposer.class);
                }
                @Override
                public String toString() { return "HeuristicPlanner+EvenDecomposer+InMemoryHashJoinResults"; }
            },
            new AbstractModule() {
                @Override
                protected void configure() {
                    bind(JoinNodeExecutor.class).to(FixedHashJoinNodeExecutor.class);
                    bind(HashJoinResultsFactory.class).toInstance(ParallelInMemoryHashJoinResults::new);
                    bind(Planner.class).to(HeuristicPlanner.class);
                    bind(DecompositionStrategy.class).to(EvenDecomposer.class);
                }
                @Override
                public String toString() { return "HeuristicPlanner+EvenDecomposer+ParallelInMemoryHashJoinResults"; }
            }
    );

    private static @Nonnull Object[][] prependModules(List<List<Object>> in) {
        List<List<Object>> rows = new ArrayList<>();
        for (Module module : moduleList) {
            for (List<Object> list : in) {
                ArrayList<Object> row = new ArrayList<>();
                row.add(module);
                row.addAll(list);
                rows.add(row);
            }
        }
        return rows.stream().map(List::toArray).toArray(Object[][]::new);
    }

    public static List<List<Object>> singleTripleData() {
        return asList(
                asList(new SetupSingleEp(), CQuery.from(new Triple(X, knows, BOB)),
                       newHashSet(MapSolution.build(X, ALICE))),
                asList(new SetupSingleEp(), CQuery.from(new Triple(X, knows, Y)),
                       newHashSet(MapSolution.builder().put(X, ALICE).put(Y, BOB).build())),
                asList(new SetupTwoEps(), CQuery.from(new Triple(X, knows, BOB)),
                       newHashSet(MapSolution.build(X, ALICE),
                                  MapSolution.build(X, DAVE))),
                asList(new SetupTwoEps(), CQuery.from(new Triple(X, knows, BOB)),
                       newHashSet(MapSolution.build(X, ALICE),
                                  MapSolution.build(X, DAVE))),
                asList(new SetupTwoEps(), CQuery.from(new Triple(X, knows, Y)),
                        newHashSet(MapSolution.builder().put(X, ALICE).put(Y, BOB).build(),
                                   MapSolution.builder().put(X,  DAVE).put(Y, BOB).build()))
        );
    }

    public static List<List<Object>> singleEpQueryData() {
        return asList(
                asList(new SetupSingleEp(),
                        CQuery.from(asList(new Triple(X, knows, BOB),
                                           new Triple(X, age, i23))),
                        newHashSet(MapSolution.build(X, ALICE))),
                asList(new SetupSingleEp(),
                       CQuery.from(asList(new Triple(X, knows, Y),
                                          new Triple(X, age, i23))),
                       newHashSet(MapSolution.builder().put(X, ALICE).put(Y, BOB).build())),
                asList(new SetupTwoEps(),
                        CQuery.from(asList(new Triple(X, knows, BOB),
                                           new Triple(X, age, i23))),
                        newHashSet(MapSolution.build(X, ALICE))),
                asList(new SetupTwoEps(),
                        CQuery.from(asList(new Triple(X, knows, Y),
                                           new Triple(X, age, i23))),
                        newHashSet(MapSolution.builder().put(X, ALICE).put(Y, BOB).build())),
                asList(new SetupTwoEps(),
                        CQuery.from(asList(new Triple(X, knows, Y),
                                           new Triple(X, age, i25))),
                        newHashSet(MapSolution.builder().put(X, DAVE).put(Y, BOB).build()))
        );
    }

    public static List<List<Object>> crossEpJoinsData() {
        return asList(
                asList(new SetupBookShop(),
                       CQuery.from(asList(new Triple(X, author, Y),
                                          new Triple(Y, name, author1))),
                       newHashSet(MapSolution.builder().put(X, ex("books/1"))
                                                       .put(Y, ex("authors/1")).build())),
                asList(new SetupBookShop(),
                       CQuery.with(asList(new Triple(X, author, Y),
                                          new Triple(Y, name, author1))).project(X).build(),
                        newHashSet(MapSolution.build(X, ex("books/1")))),
                asList(new SetupBookShop(),
                        CQuery.from(asList(new Triple(X, author, Y),
                                           new Triple(X, genre, Z),
                                           new Triple(Y, name, author1),
                                           new Triple(Z, genreName, genre1))),
                        newHashSet(MapSolution.builder().put(X, ex("books/1"))
                                                        .put(Y, ex("authors/1"))
                                                        .put(Z, ex("genres/1")).build())),
                asList(new SetupBookShop(),
                        CQuery.with(asList(new Triple(X, author, Y),
                                           new Triple(X, genre, Z),
                                           new Triple(Y, name, author1),
                                           new Triple(Z, genreName, genre1))).project(X).build(),
                        newHashSet(MapSolution.build(X, ex("books/1")))),
                asList(new SetupBookShop(),
                        CQuery.with(asList(new Triple(X, author, Y),
                                           new Triple(X, genre, Z),
                                           new Triple(Y, name, author1),
                                           new Triple(Z, genreName, genre1))).project(Y).build(),
                        newHashSet(MapSolution.build(Y, ex("authors/1")))),
                asList(new SetupBookShop(),
                       CQuery.from(asList(new Triple(X, title, title1),
                                          new Triple(X, genre, Y),
                                          new Triple(Y, genreName, Z))),
                       newHashSet(MapSolution.builder().put(X, ex("books/1"))
                                                       .put(Y, ex("genres/1"))
                                                       .put(Z, genre1).build())),
                asList(new SetupBookShop(),
                       CQuery.with(asList(new Triple(X, title, title1),
                                          new Triple(X, genre, Y),
                                          new Triple(Y, genreName, Z))).project(X).build(),
                       newHashSet(MapSolution.build(X, ex("books/1"))))
        );
    }

    @DataProvider
    public static Object[][] queryData() {
        List<List<Object>> list = new ArrayList<>();
        list.addAll(singleTripleData());
        list.addAll(singleEpQueryData());
        list.addAll(crossEpJoinsData());
        return prependModules(list);
    }

    @Test(dataProvider = "queryData")
    public void testQuery(@Nonnull Module module, @Nonnull Consumer<Federation> setup,
                          @Nonnull CQuery query,  @Nonnull Set<Solution> expected) {
        Injector injector = Guice.createInjector(Modules.override(new SimpleExecutionModule())
                                                        .with(module));
        Federation federation = injector.getInstance(Federation.class);
        setup.accept(federation);

        Set<Solution> actual = new HashSet<>();
        try (Results results = federation.query(query)) {
            results.forEachRemaining(actual::add);
        }
        assertEquals(actual, expected);
    }
}