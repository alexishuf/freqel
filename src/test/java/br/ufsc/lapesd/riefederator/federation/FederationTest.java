package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.description.AskDescription;
import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.EvenDecomposer;
import br.ufsc.lapesd.riefederator.federation.execution.tree.JoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.FixedBindJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.FixedHashJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.SimpleJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.HashJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.ParallelInMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.PlannerTest;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Res;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.ModelMessageBodyWriter;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.UriTemplateExecutor;
import com.google.inject.Module;
import com.google.inject.*;
import com.google.inject.util.Modules;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.uri.UriTemplate;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.*;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.jena.rdf.model.ResourceFactory.createLangLiteral;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class FederationTest extends JerseyTestNg.ContainerPerClassTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI DAVE = new StdURI("http://example.org/Dave");
    public static final @Nonnull StdURI type = new StdURI(RDF.type.getURI());
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
    public static final @Nonnull StdVar W = new StdVar("w");

    private static @Nonnull StdURI ex(@Nonnull String local) {
        return new StdURI("http://example.org/"+local);
    }

    private static  @Nonnull ARQEndpoint createEndpoint(@Nonnull String filename) {
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, FederationTest.class.getResourceAsStream("../"+filename), Lang.TTL);
        return ARQEndpoint.forModel(m);
    }

    private static @Nonnull Source createWebAPISource(@Nonnull Molecule molecule,
                                                      @Nonnull String base, @Nonnull String file) {
        UriTemplate uriTemplate = new UriTemplate(base+"/describe/"+file+"{?uri}");
        UriTemplateExecutor executor = new UriTemplateExecutor(uriTemplate);
        Map<String, String> atom2input = new HashMap<>();
        atom2input.put(molecule.getCore().getName(), "uri");
        APIMolecule apiMolecule = new APIMolecule(molecule, executor, atom2input);
        WebAPICQEndpoint endpoint = new WebAPICQEndpoint(apiMolecule);
        return new Source(endpoint.getMatcher(), endpoint);
    }

    private static @Nonnull Source
    createSubjectsWebAPISource(@Nonnull Molecule molecule, @Nonnull String base,
                               @Nonnull String file, @Nonnull StdURI predicate,
                               @Nonnull String inAtomName) {
        String predEnc;
        try {
            predEnc = URLEncoder.encode(predicate.getURI(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        UriTemplate uriTemplate = new UriTemplate(base+"describeSubjects/"+file+"{?uri}&pred="+predEnc);
        UriTemplateExecutor executor = new UriTemplateExecutor(uriTemplate);
        Map<String, String> atom2input = new HashMap<>();
        assertTrue(molecule.getAtomMap().containsKey(inAtomName));
        atom2input.put(inAtomName, "uri");
        APIMolecule apiMolecule = new APIMolecule(molecule, executor, atom2input);
        WebAPICQEndpoint endpoint = new WebAPICQEndpoint(apiMolecule);
        return new Source(endpoint.getMatcher(), endpoint);
    }

    @Path("/")
    public static class Service {
        @GET
        @Path("describe/{file}")
        public @Nonnull Model describe(@PathParam("file") String file,
                                       @QueryParam("uri") String uri) {
            Model out = ModelFactory.createDefaultModel();
            ARQEndpoint src = createEndpoint(file);
            Resource subj = createResource(uri);
            try (Results results = src.query(CQuery.from(new Triple(fromJena(subj), X, Y)))) {
                while (results.hasNext()) {
                    Solution solution = results.next();
                    out.add(subj, toJenaProperty(solution.get("x")), toJena(solution.get("y")));
                }
            }
            return out;
        }
        @GET
        @Path("describeSubjects/{file}")
        public @Nonnull Model describeSubjects(@PathParam("file") String file,
                                               @QueryParam("pred") String predicateUri,
                                               @QueryParam("uri") String uri) {
            Model out = ModelFactory.createDefaultModel();
            ARQEndpoint src = createEndpoint(file);
            Resource subj = createResource(uri);
            StdURI pred = new StdURI(predicateUri);
            try (Results results = src.query(CQuery.from(new Triple(X, Y, Z),
                                                         new Triple(X, pred, fromJena(subj))))) {
                while (results.hasNext()) {
                    Solution s = results.next();
                    out.add(toJena((Res)s.get("x")), toJenaProperty(s.get("y")), toJena(s.get("z")));
                }
            }
            return out;
        }
    }

    @Override
    protected Application configure() {
        return new ResourceConfig().register(ModelMessageBodyWriter.class).register(Service.class);
    }

    @Test
    public void selfTestDescribeBooks() throws UnsupportedEncodingException {
        String encoded = URLEncoder.encode("http://example.org/books/1", "UTF-8");
        String nt = target("describe/books.nt").queryParam("uri", encoded)
                .request("application/n-triples").get(String.class);
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, new StringReader(nt), null, Lang.NT);

        Model expected = ModelFactory.createDefaultModel();
        Resource s = toJena(ex("books/1"));
        expected.add(s, RDF.type,                           toJena(ex("Book")));
        expected.add(s, toJenaProperty(ex("title")),  createLangLiteral("title 1", "en"));
        expected.add(s, toJenaProperty(ex("author")), toJena(ex("authors/1")));
        expected.add(s, toJenaProperty(ex("genre")),  toJena(ex("genres/1")));

        assertEquals(model.size(), expected.size());
        assertTrue(model.isIsomorphicWith(expected));
    }

    @Test
    public void selfTestDescribeBookByGenre() throws UnsupportedEncodingException {
        String predEnc = URLEncoder.encode("http://example.org/genre", "UTF-8");
        String genreEnc = URLEncoder.encode("http://example.org/genres/2", "UTF-8");
        String nt = target("describeSubjects/books.nt")
                .queryParam("uri", genreEnc)
                .queryParam("pred", predEnc)
                .request("application/n-triples").get(String.class);
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, new StringReader(nt), null, Lang.NT);

        Model expected = ModelFactory.createDefaultModel();
        Resource s2 = toJena(ex("books/2"));
        expected.add(s2, RDF.type,                           toJena(ex("Book")));
        expected.add(s2, toJenaProperty(ex("title")),  createLangLiteral("title 2", "en"));
        expected.add(s2, toJenaProperty(ex("author")), toJena(ex("authors/2")));
        expected.add(s2, toJenaProperty(ex("genre")),  toJena(ex("genres/2")));

        Resource s4 = toJena(ex("books/4"));
        expected.add(s4, RDF.type,                           toJena(ex("Book")));
        expected.add(s4, toJenaProperty(ex("title")),  createLangLiteral("title 4", "en"));
        expected.add(s4, toJenaProperty(ex("author")), toJena(ex("authors/4")));
        expected.add(s4, toJenaProperty(ex("genre")),  toJena(ex("genres/2")));

        assertEquals(model.size(), expected.size());
        assertTrue(model.isIsomorphicWith(expected));
    }

    private interface Setup extends BiConsumer<Federation, String> {
        default @Nullable Setup nextVariant() {return null;}
        boolean requiresBindJoin();
    }

    private static final class SetupSingleEp implements Setup {
        @Override
        public void accept(@Nonnull Federation federation, @Nonnull String baseServiceUri) {
            ARQEndpoint ep = createEndpoint("rdf-1.nt");
            federation.addSource(new Source(new SelectDescription(ep), ep));
        }
        @Override
        public boolean requiresBindJoin() { return false; }
        @Override
        public String toString() { return "SetupSingleEp"; }
    }

    private static final class SetupTwoEps implements Setup {
        @Override
        public void accept(@Nonnull Federation federation, @Nonnull String baseServiceUri) {
            ARQEndpoint ep = createEndpoint("rdf-1.nt");
            federation.addSource(new Source(new SelectDescription(ep), ep));
            ep = createEndpoint("rdf-2.nt");
            federation.addSource(new Source(new AskDescription(ep), ep));
        }
        @Override
        public boolean requiresBindJoin() { return false; }
        @Override
        public String toString() { return "SetupTwoEps";}
    }

    private static final class SetupBookShop implements Setup {
        private int variantIdx;

        public SetupBookShop(int variantIdx) {
            this.variantIdx = variantIdx;
        }

        @Override
        public @Nullable Setup nextVariant() {
            return variantIdx >= 1 ? null : new SetupBookShop(variantIdx + 1);
        }
        @Override
        public boolean requiresBindJoin() { return variantIdx > 0; }
        @Override
        public void accept(Federation federation, @Nonnull String base) {
            ARQEndpoint authors = createEndpoint("authors.nt");
            federation.addSource(new Source(new SelectDescription(authors), authors));
            if (variantIdx == 1) {
                Molecule bookMolecule = Molecule.builder("Book")
                        .out(type, Molecule.builder("class").buildAtom())
                        .out(ex("title"), Molecule.builder("title").buildAtom())
                        .out(ex("author"), Molecule.builder("author").buildAtom())
                        .out(ex("genre"), Molecule.builder("genre").buildAtom())
                        .exclusive().build();
                federation.addSource(createWebAPISource(bookMolecule, base, "books.nt"));
                federation.addSource(createSubjectsWebAPISource(bookMolecule, base,
                        "books.nt", ex("author"), "author"));
                federation.addSource(createSubjectsWebAPISource(bookMolecule, base,
                        "books.nt", ex("genre"), "genre"));
            } else {
                ARQEndpoint books = createEndpoint("books.nt");
                federation.addSource(new Source(new SelectDescription(books), books));
            }
            ARQEndpoint genres = createEndpoint("genres.nt");
            federation.addSource(new Source(new SelectDescription(genres), genres));
        }
        @Override
        public String toString() {
            return asList("SetupBookShop",
                          "SetupBookShop+authorsAPI",
                          "SetupBookShop+booksAPI",
                          "SetupBookShop+genresAPI").get(variantIdx);
        }
    }

    private static abstract class TestModule extends AbstractModule {
        private boolean canBindJoin;
        public TestModule(boolean canBindJoin) {this.canBindJoin = canBindJoin;}
        public boolean canBindJoin() { return canBindJoin; }
    }

    private static final @Nonnull
    List<Function<Provider<Planner>, TestModule>> moduleProtoList = asList(
            planner -> new TestModule(true) {
                @Override
                protected void configure() {
                    bind(Planner.class).toProvider(planner);
                    bind(DecompositionStrategy.class).to(EvenDecomposer.class);
                }
                @Override
                public String toString() { return planner+"+even"; }
            },
            planner -> new TestModule(false) {
                @Override
                protected void configure() {
                    bind(JoinNodeExecutor.class).to(FixedHashJoinNodeExecutor.class);
                    bind(HashJoinResultsFactory.class).toInstance(InMemoryHashJoinResults::new);
                    bind(Planner.class).toProvider(planner);
                    bind(DecompositionStrategy.class).to(EvenDecomposer.class);
                }
                @Override
                public String toString() { return planner+"+even+InMemoryHashJoinResults"; }
            },
            planner -> new TestModule(false) {
                @Override
                protected void configure() {
                    bind(JoinNodeExecutor.class).to(FixedHashJoinNodeExecutor.class);
                    bind(HashJoinResultsFactory.class).toInstance(ParallelInMemoryHashJoinResults::new);
                    bind(Planner.class).toProvider(planner);
                    bind(DecompositionStrategy.class).to(EvenDecomposer.class);
                }
                @Override
                public String toString() { return planner+"+even+ParallelInMemoryHashJoinResults"; }
            },
            planner -> new TestModule(true) {
                @Override
                protected void configure() {
                    bind(JoinNodeExecutor.class).to(FixedBindJoinNodeExecutor.class);
                    bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
                    bind(Planner.class).toProvider(planner);
                    bind(DecompositionStrategy.class).to(EvenDecomposer.class);
                }
                @Override
                public String toString() { return planner+"+even+SimpleBindJoinResults"; }
            },
            planner -> new TestModule(true) {
                @Override
                protected void configure() {
                    bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
                    bind(JoinNodeExecutor.class).to(SimpleJoinNodeExecutor.class);
                    bind(Planner.class).toProvider(planner);
                    bind(DecompositionStrategy.class).to(EvenDecomposer.class);
                }
                @Override
                public String toString() { return planner+"+even+SimpleJoinNodeExecutor"; }
            }
    );

    private static final @Nonnull List<TestModule> moduleList = PlannerTest.suppliers.stream()
            .flatMap(ns -> moduleProtoList.stream().map(p -> p.apply(ns)))
            .collect(toList());

    private static @Nonnull Object[][] prependModules(List<List<Object>> in) {
        List<List<Object>> rows = new ArrayList<>();
        for (TestModule module : moduleList) {
            for (List<Object> list : in) {
                if (((Setup)list.get(0)).requiresBindJoin() && !module.canBindJoin())
                    continue; //skip
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
                asList(new SetupBookShop(0),
                       CQuery.from(asList(new Triple(X, author, Y),
                                          new Triple(Y, name, author1))),
                       newHashSet(MapSolution.builder().put(X, ex("books/1"))
                                                       .put(Y, ex("authors/1")).build())),
                asList(new SetupBookShop(0),
                       CQuery.with(asList(new Triple(X, author, Y),
                                          new Triple(Y, name, author1))).project(X).build(),
                        newHashSet(MapSolution.build(X, ex("books/1")))),
                asList(new SetupBookShop(0),
                        CQuery.from(asList(new Triple(X, author, Y),
                                           new Triple(X, genre, Z),
                                           new Triple(Y, name, author1),
                                           new Triple(Z, genreName, genre1))),
                        newHashSet(MapSolution.builder().put(X, ex("books/1"))
                                                        .put(Y, ex("authors/1"))
                                                        .put(Z, ex("genres/1")).build())),
                asList(new SetupBookShop(0),
                        CQuery.with(asList(new Triple(X, author, Y),
                                           new Triple(X, genre, Z),
                                           new Triple(Y, name, author1),
                                           new Triple(Z, genreName, genre1))).project(X).build(),
                        newHashSet(MapSolution.build(X, ex("books/1")))),
                asList(new SetupBookShop(0),
                        CQuery.with(asList(new Triple(X, author, Y),
                                           new Triple(X, genre, Z),
                                           new Triple(Y, name, author1),
                                           new Triple(Z, genreName, genre1))).project(Y).build(),
                        newHashSet(MapSolution.build(Y, ex("authors/1")))),
                asList(new SetupBookShop(0),
                       CQuery.from(asList(new Triple(X, title, title1),
                                          new Triple(X, genre, Y),
                                          new Triple(Y, genreName, Z))),
                       newHashSet(MapSolution.builder().put(X, ex("books/1"))
                                                       .put(Y, ex("genres/1"))
                                                       .put(Z, genre1).build())),
                asList(new SetupBookShop(0),
                       CQuery.with(asList(new Triple(X, title, title1),
                                          new Triple(X, genre, Y),
                                          new Triple(Y, genreName, Z))).project(X).build(),
                       newHashSet(MapSolution.build(X, ex("books/1"))))
        );
    }

    @DataProvider
    public static Object[][] queryData() {
        List<List<Object>> basic = new ArrayList<>();
        basic.addAll(singleTripleData());
        basic.addAll(singleEpQueryData());
        basic.addAll(crossEpJoinsData());

        List<List<Object>> withVariants = new ArrayList<>();
        for (List<Object> row : basic) {
            withVariants.add(row);
            Setup variant = (Setup) row.get(0);
            while ((variant = variant.nextVariant()) != null) {
                ArrayList<Object> copy = new ArrayList<>(row);
                copy.set(0, variant);
                withVariants.add(copy);
            }
        }
        return prependModules(withVariants);
    }

    @Test(dataProvider = "queryData")
    public void testQuery(@Nonnull Module module, @Nonnull Setup setup,
                          @Nonnull CQuery query,  @Nonnull Set<Solution> expected) {
        Injector injector = Guice.createInjector(Modules.override(new SimpleExecutionModule())
                                                        .with(module));
        Federation federation = injector.getInstance(Federation.class);
        setup.accept(federation, target().getUri().toString());

        Set<Solution> actual = new HashSet<>();
        try (Results results = federation.query(query)) {
            results.forEachRemaining(actual::add);
        }
        assertEquals(actual, expected);
    }
}