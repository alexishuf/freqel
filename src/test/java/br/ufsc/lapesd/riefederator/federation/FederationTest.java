package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.AskDescription;
import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.EvenDecomposer;
import br.ufsc.lapesd.riefederator.federation.decomp.StandardDecomposer;
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
import org.apache.jena.vocabulary.RDF;
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
import java.util.function.BiFunction;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.*;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.jena.rdf.model.ResourceFactory.createLangLiteral;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class FederationTest extends JerseyTestNg.ContainerPerClassTest implements TestContext {
    public static final @Nonnull StdLit title1 = StdLit.fromUnescaped("title 1", "en");
    public static final @Nonnull StdLit author1 = StdLit.fromUnescaped("author1", "en");
    public static final @Nonnull StdLit genre1 = StdLit.fromUnescaped("genre1", "en");
    public static final @Nonnull StdLit i23 = StdLit.fromUnescaped("23", xsdInt);
    public static final @Nonnull StdLit i25 = StdLit.fromUnescaped("25", xsdInt);

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
            try (Results results = src.query(CQuery.from(new Triple(fromJena(subj), x, y)))) {
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
            try (Results results = src.query(CQuery.from(new Triple(x, y, z),
                                                         new Triple(x, pred, fromJena(subj))))) {
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

    private static final @Nonnull List<BiFunction<Provider<Planner>,
                                       Class<? extends DecompositionStrategy>,
                                       TestModule>> moduleProtoList = asList(
            (planner, decomp) -> new TestModule(true) {
                @Override
                protected void configure() {
                    bind(Planner.class).toProvider(planner);
                    bind(DecompositionStrategy.class).to(decomp);
                }
                @Override
                public String toString() { return planner+"+"+decomp; }
            },
            (planner, decomp) -> new TestModule(false) {
                @Override
                protected void configure() {
                    bind(JoinNodeExecutor.class).to(FixedHashJoinNodeExecutor.class);
                    bind(HashJoinResultsFactory.class).toInstance(InMemoryHashJoinResults::new);
                    bind(Planner.class).toProvider(planner);
                    bind(DecompositionStrategy.class).to(decomp);
                }
                @Override
                public String toString() { return planner+"+"+decomp+"+InMemoryHashJoinResults"; }
            },
            (planner, decomp) -> new TestModule(false) {
                @Override
                protected void configure() {
                    bind(JoinNodeExecutor.class).to(FixedHashJoinNodeExecutor.class);
                    bind(HashJoinResultsFactory.class).toInstance(ParallelInMemoryHashJoinResults::new);
                    bind(Planner.class).toProvider(planner);
                    bind(DecompositionStrategy.class).to(decomp);
                }
                @Override
                public String toString() { return planner+"+"+decomp+"+ParallelInMemoryHashJoinResults"; }
            },
            (planner, decomp) -> new TestModule(true) {
                @Override
                protected void configure() {
                    bind(JoinNodeExecutor.class).to(FixedBindJoinNodeExecutor.class);
                    bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
                    bind(Planner.class).toProvider(planner);
                    bind(DecompositionStrategy.class).to(decomp);
                }
                @Override
                public String toString() { return planner+"+"+decomp+"+SimpleBindJoinResults"; }
            },
            (planner, decomp) -> new TestModule(true) {
                @Override
                protected void configure() {
                    bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
                    bind(JoinNodeExecutor.class).to(SimpleJoinNodeExecutor.class);
                    bind(Planner.class).toProvider(planner);
                    bind(DecompositionStrategy.class).to(decomp);
                }
                @Override
                public String toString() { return planner+"+"+decomp+"+SimpleJoinNodeExecutor"; }
            }
    );

    private static final @Nonnull List<Class<? extends DecompositionStrategy>>
            decomposerClasses = asList(EvenDecomposer.class, StandardDecomposer.class);

    private static final @Nonnull List<TestModule> moduleList = PlannerTest.suppliers.stream()
            .flatMap(ps -> decomposerClasses.stream()
                    .flatMap(dc -> moduleProtoList.stream().map(p -> p.apply(ps, dc))))
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
                asList(new SetupSingleEp(), CQuery.from(new Triple(x, knows, Bob)),
                       newHashSet(MapSolution.build(x, Alice))),
                asList(new SetupSingleEp(), CQuery.from(new Triple(x, knows, y)),
                       newHashSet(MapSolution.builder().put(x, Alice).put(y, Bob).build())),
                asList(new SetupTwoEps(), CQuery.from(new Triple(x, knows, Bob)),
                       newHashSet(MapSolution.build(x, Alice),
                                  MapSolution.build(x, Dave))),
                asList(new SetupTwoEps(), CQuery.from(new Triple(x, knows, Bob)),
                       newHashSet(MapSolution.build(x, Alice),
                                  MapSolution.build(x, Dave))),
                asList(new SetupTwoEps(), CQuery.from(new Triple(x, knows, y)),
                        newHashSet(MapSolution.builder().put(x, Alice).put(y, Bob).build(),
                                   MapSolution.builder().put(x, Dave).put(y, Bob).build()))
        );
    }

    public static List<List<Object>> singleEpQueryData() {
        return asList(
                asList(new SetupSingleEp(),
                        CQuery.from(asList(new Triple(x, knows, Bob),
                                           new Triple(x, age, i23))),
                        newHashSet(MapSolution.build(x, Alice))),
                asList(new SetupSingleEp(),
                       CQuery.from(asList(new Triple(x, knows, y),
                                          new Triple(x, age, i23))),
                       newHashSet(MapSolution.builder().put(x, Alice).put(y, Bob).build())),
                asList(new SetupTwoEps(),
                        CQuery.from(asList(new Triple(x, knows, Bob),
                                           new Triple(x, age, i23))),
                        newHashSet(MapSolution.build(x, Alice))),
                asList(new SetupTwoEps(),
                        CQuery.from(asList(new Triple(x, knows, y),
                                           new Triple(x, age, i23))),
                        newHashSet(MapSolution.builder().put(x, Alice).put(y, Bob).build())),
                asList(new SetupTwoEps(),
                        CQuery.from(asList(new Triple(x, knows, y),
                                           new Triple(x, age, i25))),
                        newHashSet(MapSolution.builder().put(x, Dave).put(y, Bob).build()))
        );
    }

    public static List<List<Object>> crossEpJoinsData() {
        return asList(
                asList(new SetupBookShop(0),
                       CQuery.from(asList(new Triple(x, author, y),
                                          new Triple(y, nameEx, author1))),
                       newHashSet(MapSolution.builder().put(x, ex("books/1"))
                                                       .put(y, ex("authors/1")).build())),
                asList(new SetupBookShop(0),
                       CQuery.with(asList(new Triple(x, author, y),
                                          new Triple(y, nameEx, author1))).project(x).build(),
                        newHashSet(MapSolution.build(x, ex("books/1")))),
                asList(new SetupBookShop(0),
                        CQuery.from(asList(new Triple(x, author, y),
                                           new Triple(x, genre, z),
                                           new Triple(y, nameEx, author1),
                                           new Triple(z, genreName, genre1))),
                        newHashSet(MapSolution.builder().put(x, ex("books/1"))
                                                        .put(y, ex("authors/1"))
                                                        .put(z, ex("genres/1")).build())),
                asList(new SetupBookShop(0),
                        CQuery.with(asList(new Triple(x, author, y),
                                           new Triple(x, genre, z),
                                           new Triple(y, nameEx, author1),
                                           new Triple(z, genreName, genre1))).project(x).build(),
                        newHashSet(MapSolution.build(x, ex("books/1")))),
                asList(new SetupBookShop(0),
                        CQuery.with(asList(new Triple(x, author, y),
                                           new Triple(x, genre, z),
                                           new Triple(y, nameEx, author1),
                                           new Triple(z, genreName, genre1))).project(y).build(),
                        newHashSet(MapSolution.build(y, ex("authors/1")))),
                asList(new SetupBookShop(0),
                       CQuery.from(asList(new Triple(x, title, title1),
                                          new Triple(x, genre, y),
                                          new Triple(y, genreName, z))),
                       newHashSet(MapSolution.builder().put(x, ex("books/1"))
                                                       .put(y, ex("genres/1"))
                                                       .put(z, genre1).build())),
                asList(new SetupBookShop(0),
                       CQuery.with(asList(new Triple(x, title, title1),
                                          new Triple(x, genre, y),
                                          new Triple(y, genreName, z))).project(x).build(),
                       newHashSet(MapSolution.build(x, ex("books/1"))))
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