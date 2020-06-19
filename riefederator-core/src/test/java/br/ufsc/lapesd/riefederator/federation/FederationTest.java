package br.ufsc.lapesd.riefederator.federation;

import br.ufsc.lapesd.riefederator.LargeRDFBenchSelfTest;
import br.ufsc.lapesd.riefederator.NamedSupplier;
import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.AskDescription;
import br.ufsc.lapesd.riefederator.description.Description;
import br.ufsc.lapesd.riefederator.description.SelectDescription;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.description.semantic.SemanticSelectDescription;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.cardinality.CardinalityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.EstimatePolicy;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.GeneralSelectivityHeuristic;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.NoCardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.cardinality.impl.WorstCaseCardinalityEnsemble;
import br.ufsc.lapesd.riefederator.federation.decomp.DecompositionStrategy;
import br.ufsc.lapesd.riefederator.federation.decomp.EvenDecomposer;
import br.ufsc.lapesd.riefederator.federation.decomp.StandardDecomposer;
import br.ufsc.lapesd.riefederator.federation.execution.tree.CartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.JoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.EagerCartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleExecutionModule;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.FixedBindJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.FixedHashJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.HashJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.ParallelInMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.riefederator.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.riefederator.federation.planner.OuterPlanner;
import br.ufsc.lapesd.riefederator.federation.planner.OuterPlannerTest;
import br.ufsc.lapesd.riefederator.federation.planner.Planner;
import br.ufsc.lapesd.riefederator.federation.planner.PlannerTest;
import br.ufsc.lapesd.riefederator.federation.planner.impl.JoinOrderPlanner;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.linkedator.Linkedator;
import br.ufsc.lapesd.riefederator.linkedator.LinkedatorResult;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Res;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TPEndpointTest;
import br.ufsc.lapesd.riefederator.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.riefederator.query.endpoint.impl.SPARQLClient;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLParseException;
import br.ufsc.lapesd.riefederator.query.parse.SPARQLQueryParser;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.BufferedResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.results.impl.SequentialResultsExecutor;
import br.ufsc.lapesd.riefederator.reason.tbox.OWLAPITBoxReasoner;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxReasoner;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.riefederator.reason.tbox.TransitiveClosureTBoxReasoner;
import br.ufsc.lapesd.riefederator.webapis.TransparencyService;
import br.ufsc.lapesd.riefederator.webapis.TransparencyServiceTestContext;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.ModelMessageBodyWriter;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.UriTemplateExecutor;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.util.Modules;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.uri.UriTemplate;
import org.jetbrains.annotations.NotNull;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static br.ufsc.lapesd.riefederator.LargeRDFBenchSelfTest.*;
import static br.ufsc.lapesd.riefederator.federation.SimpleFederationModule.configureCardinalityEstimation;
import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.*;
import static br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint.forModel;
import static br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint.forService;
import static br.ufsc.lapesd.riefederator.webapis.TransparencyService.*;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.jena.rdf.model.ResourceFactory.*;
import static org.testng.Assert.*;

public class FederationTest extends JerseyTestNg.ContainerPerClassTest
        implements TestContext, TransparencyServiceTestContext {
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
        return forModel(m);
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
        return new ResourceConfig().register(ModelMessageBodyWriter.class)
                .register(Service.class)
                .register(TransparencyService.class);
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
        boolean requiresFastModule();
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
        public boolean requiresFastModule() { return false; }
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
        public boolean requiresFastModule() { return false; }
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
        public boolean requiresFastModule() { return false; }
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

    private static final class SetupTransparency implements Setup {
        @Override
        public boolean requiresBindJoin() {
            return true;
        }
        @Override
        public boolean requiresFastModule() { return false; }

        @Override
        public void accept(Federation federation, String base) {
            WebTarget target = ClientBuilder.newClient().target(base);
            try {
                federation.addSource(getContractsClient(target).asSource());
                federation.addSource(getContractByIdClient(target).asSource());
                federation.addSource(getProcurementsClient(target).asSource());
                federation.addSource(getProcurementsByIdClient(target).asSource());
                federation.addSource(getProcurementByNumberClient(target).asSource());
                federation.addSource(getOrgaosSiafiClient(target).asSource());
                ARQEndpoint ep = createEndpoint("modalidades.ttl");
                federation.addSource(new Source(new SelectDescription(ep), ep));
                //some completely unrelated endpoints...
                ep = createEndpoint("books.nt");
                federation.addSource(new Source(new SelectDescription(ep), ep));
                ep = createEndpoint("genres.nt");
                federation.addSource(new Source(new SelectDescription(ep), ep));

                Linkedator linkedator = Linkedator.getDefault();
                List<LinkedatorResult> suggestions;
                suggestions = linkedator.getSuggestions(federation.getSources());
                List<LinkedatorResult> hasLicitacaoSuggestions = suggestions.stream()
                        .filter(r -> r.getTemplateLink().equals(hasLicitacao.asURI()))
                        .collect(toList());
                assertEquals(hasLicitacaoSuggestions.size(), 1);
                linkedator.install(federation, hasLicitacaoSuggestions);

            } catch (IOException e) {
                fail("Unexpected exception", e);
            }
        }

        @Override
        public String toString() {
            return "SetupTransparency";
        }
    }

    private final static class SetupLargeRDFBench implements Setup {
        private final int variantIdx;
        private final String[] variantNames = {
                /* 0 */ "SelectDescription+ARQEndpoint",
                /* 1 */ "SelectDescription+fetchClasses+ARQEndpoint",
                /* 2 */ "AskDescription+ARQEndpoint",
                /* 3 */ "SemanticSelectDescription+ARQEndpoint",
                /* 4 */ "SelectDescription+ARQEndpoint+union",
                /* 5 */ "SelectDescription+fetchClasses+SPARQLClient",
                /* 6 */ "AskDescription+SPARQLClient",
                /* 7 */ "SelectDescription+SPARQLClient+union",
        };

        public SetupLargeRDFBench(int variantIdx) {
            this.variantIdx = variantIdx;
        }

        @Override
        public @Nullable Setup nextVariant() {
            return variantIdx >= variantNames.length-1
                    ? null : new SetupLargeRDFBench(variantIdx + 1);
        }

        @Override
        public boolean requiresBindJoin() {
            return false;
        }
        @Override
        public boolean requiresFastModule() { return true; }

        private @Nonnull Source wrap(@Nonnull Model model, @Nonnull String endpointName) {
            String variantName = variantNames[variantIdx];
            CQEndpoint ep = null;
            Description description = null;
            if (variantName.contains("SPARQLClient")) {
                String key = "SetupLargeRDFBench-"+endpointName;
                TPEndpointTest.FusekiEndpoint fuseki = fusekiEndpoints.computeIfAbsent(key, k ->
                        new TPEndpointTest.FusekiEndpoint(DatasetFactory.create(model)));
                ep = new SPARQLClient(fuseki.uri);
            } else if (variantName.contains("ARQEndpoint")) {
                ep = ARQEndpoint.forModel(model, endpointName);
            }
            assert ep != null;

            if (variantName.contains("SelectDescription+fetchClasses"))
                description = new SelectDescription(ep, true);
            else if (variantName.contains("SelectDescription"))
                description = new SelectDescription(ep, false);
            else if (variantName.contains("AskDescription"))
                description = new AskDescription(ep);
            else if (variantName.contains("SemanticSelectDescription"))
                description = new SemanticSelectDescription(ep, true, new TransitiveClosureTBoxReasoner());
            assert description != null;

            Source source = new Source(description, ep);
            if (variantName.contains("SPARQLClient"))
                source.setCloseEndpoint(true);
            return source;
        }

        @Override
        public void accept(Federation federation, String base) {
            String variantName = variantNames[variantIdx];

            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Model union = ModelFactory.createDefaultModel();
            for (String filename : DATA_FILENAMES) {
                try (InputStream stream = cl.getResourceAsStream(RESOURCE_DIR + "/data/" + filename)) {
                    assertNotNull(stream);
                    if (variantName.contains("union")) {
                        RDFDataMgr.read(union, stream, Lang.NT);
                    } else {
                        Model model = ModelFactory.createDefaultModel();
                        RDFDataMgr.read(model, stream, Lang.NT);
                        federation.addSource(wrap(model, filename));
                    }
                } catch (IOException e) {
                    fail("Unexpected exception", e);
                }
            }
            if (variantName.contains("union"))
                federation.addSource(wrap(union, "LargeRDFBench-union"));
        }

        @Override
        public @Nonnull String toString() {
            return "SetupLargeRDFBench["+variantNames[variantIdx]+"]";
        }
    }

    private static abstract class TestModule extends AbstractModule {
        private final boolean canBindJoin;
        private boolean storingPerformanceListener;
        private Class<? extends OuterPlanner> op;
        private Class<? extends Planner> ip;
        private Class<? extends JoinOrderPlanner> jo;
        private Class<? extends DecompositionStrategy> ds;
        private Class<? extends PerformanceListener> pl;

        public TestModule(boolean canBindJoin,
                          Class<? extends OuterPlanner> op,
                          Class<? extends Planner> ip,
                          Class<? extends JoinOrderPlanner> jo,
                          Class<? extends DecompositionStrategy> ds,
                          Class<? extends PerformanceListener> pl) {
            this.canBindJoin = canBindJoin;
            this.storingPerformanceListener =
                    PerformanceListenerTest.storingClasses.contains(pl);
            this.op = op;
            this.ip = ip;
            this.jo = jo;
            this.ds = ds;
            this.pl = pl;
        }
        public boolean cannotBindJoin() { return !canBindJoin; }
        public boolean isSlowModule() { return true; }
        protected boolean canBeFast() {
            return PlannerTest.isFast(ip, jo) && fastDecomposerClasses.contains(ds);
        }
        public boolean hasStoringPerformanceListener() {
            return storingPerformanceListener;
        }

        @Override @OverridingMethodsMustInvokeSuper
        protected void configure() {
            bind(OuterPlanner.class).to(op);
            bind(Planner.class).to(ip);
            bind(JoinOrderPlanner.class).to(jo);
            bind(DecompositionStrategy.class).to(ds);
            bind(PerformanceListener.class).toInstance(new NamedSupplier<>(pl).get());
        }

        protected @Nonnull String asString(Class<?>... classes) {
            StringBuilder b = new StringBuilder();
            b.append(Stream.of(classes).map(Class::getSimpleName).collect(joining("+")));
            if (classes.length > 0)
                b.append('+');
            return b.append(ip.getSimpleName()).append('+')
                    .append(jo.getSimpleName()).append('+')
                    .append(ds.getSimpleName()).append('+')
                    .append(pl.getSimpleName()).append('+')
                    .append(op.getSimpleName()).append('+')
                    .append(isSlowModule() ? "(fast)" : "(slow)")
                    .toString();
        }
    }

    @FunctionalInterface
    private interface ModuleFactory {
        TestModule apply(Class<? extends OuterPlanner> outerPlanner,
                         Class<? extends Planner> planner,
                         Class<? extends JoinOrderPlanner> joinOrder,
                         Class<? extends DecompositionStrategy> decompositionStrategy,
                         Class<? extends PerformanceListener> performance);
    }

    private static final @Nonnull List<ModuleFactory> moduleProtoList = asList(
            (op, ip, opt, dec, pl) -> new TestModule(true, op, ip, opt, dec, pl) {
                @Override
                protected void configure() {
                    super.configure();
                    bind(CardinalityEnsemble.class).toInstance(NoCardinalityEnsemble.INSTANCE);
//                    bind(ResultsExecutor.class).toInstance(new SequentialResultsExecutor());
                }
                @Override
                public String toString() { return asString(NoCardinalityEnsemble.class); }
            },
            (op, ip, opt, dec, pl) -> new TestModule(true, op, ip, opt, dec, pl) {
                @Override
                protected void configure() {
                    super.configure();
                    bind(CardinalityEnsemble.class).to(WorstCaseCardinalityEnsemble.class);

                    Multibinder<CardinalityHeuristic> mBinder
                            = Multibinder.newSetBinder(binder(), CardinalityHeuristic.class);
                    mBinder.addBinding().to(GeneralSelectivityHeuristic.class);
//                    bind(ResultsExecutor.class).toInstance(new BufferedResultsExecutor());
                }
                @Override
                public boolean isSlowModule() {
                    return !canBeFast();
                }
                @Override
                public String toString() { return asString(GeneralSelectivityHeuristic.class, BufferedResultsExecutor.class); }
            },
            (op, ip, opt, dec, pl) -> new TestModule(true, op, ip, opt, dec, pl) {
                @Override
                protected void configure() {
                    super.configure();
                    configureCardinalityEstimation(binder(), EstimatePolicy.local(50));
                    bind(ResultsExecutor.class).toInstance(new SequentialResultsExecutor());
                }
                @Override
                public String toString() { return asString(SequentialResultsExecutor.class); }
            },
            (op, ip, opt, dec, pl) -> new TestModule(true, op, ip, opt, dec, pl) {
                @Override
                protected void configure() {
                    super.configure();
                    configureCardinalityEstimation(binder(), EstimatePolicy.local(50));
                    bind(ResultsExecutor.class).toInstance(new BufferedResultsExecutor());
                }
                @Override
                public boolean isSlowModule() {
                    return !canBeFast() || !pl.equals(NoOpPerformanceListener.class);
                }
                @Override
                public String toString() { return asString(BufferedResultsExecutor.class); }
            },
            (op, ip, opt, dec, pl) -> new TestModule(false, op, ip, opt, dec, pl) {
                @Override
                protected void configure() {
                    super.configure();
                    bind(JoinNodeExecutor.class).to(FixedHashJoinNodeExecutor.class);
                    bind(HashJoinResultsFactory.class).toInstance(InMemoryHashJoinResults::new);
                    configureCardinalityEstimation(binder(), EstimatePolicy.local(50));
//                    bind(ResultsExecutor.class).toInstance(new SequentialResultsExecutor());
                }
                @Override
                public String toString() { return asString(InMemoryHashJoinResults.class); }
            },
            (op, ip, opt, dec, pl) -> new TestModule(true, op, ip, opt, dec, pl) {
                @Override
                protected void configure() {
                    super.configure();
                    bind(CartesianNodeExecutor.class).to(EagerCartesianNodeExecutor.class);
                    configureCardinalityEstimation(binder(), 0);
                }
                @Override
                public String toString() { return asString(EagerCartesianNodeExecutor.class); }
            },
            (op, ip, opt, dec, pl) -> new TestModule(false, op, ip, opt, dec, pl) {
                @Override
                protected void configure() {
                    super.configure();
                    bind(JoinNodeExecutor.class).to(FixedHashJoinNodeExecutor.class);
                    bind(HashJoinResultsFactory.class).toInstance(InMemoryHashJoinResults::new);
                    configureCardinalityEstimation(binder(), EstimatePolicy.local(50));
//                    bind(ResultsExecutor.class).toInstance(new SequentialResultsExecutor());
                }
                @Override
                public String toString() { return asString(InMemoryHashJoinResults.class); }
            },
            (op, ip, opt, dec, pl) -> new TestModule(false, op, ip, opt, dec, pl) {
                @Override
                protected void configure() {
                    super.configure();
                    bind(JoinNodeExecutor.class).to(FixedHashJoinNodeExecutor.class);
                    bind(HashJoinResultsFactory.class).toInstance(ParallelInMemoryHashJoinResults::new);
                    configureCardinalityEstimation(binder(), EstimatePolicy.local(50));
//                    bind(ResultsExecutor.class).toInstance(new SequentialResultsExecutor());
                }
                @Override
                public String toString() { return asString(ParallelInMemoryHashJoinResults.class); }
            },
            (op, ip, opt, dec, pl) -> new TestModule(true, op, ip, opt, dec, pl) {
                @Override
                protected void configure() {
                    super.configure();
                    bind(JoinNodeExecutor.class).to(FixedBindJoinNodeExecutor.class);
                    bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
                    configureCardinalityEstimation(binder(), EstimatePolicy.local(50));
//                    bind(ResultsExecutor.class).toInstance(new SequentialResultsExecutor());
                }
                @Override
                public String toString() { return asString(SimpleBindJoinResults.class); }
            },
            (op, ip, opt, dec, pl) -> new TestModule(true, op, ip, opt, dec, pl) {
                @Override
                protected void configure() {
                    super.configure();
                    bind(JoinNodeExecutor.class).to(FixedBindJoinNodeExecutor.class);
                    bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
                    configureCardinalityEstimation(binder(), EstimatePolicy.local(50));
//                    bind(ResultsExecutor.class).toInstance(new SequentialResultsExecutor());
                }
                @Override
                public String toString() { return asString(NoCardinalityEnsemble.class, SimpleBindJoinResults.class); }
            }
    );

    private Federation federation = null;
    public static final ConcurrentHashMap<String, TPEndpointTest.FusekiEndpoint> fusekiEndpoints
            = new ConcurrentHashMap<>();

    @AfterMethod
    public void methodTearDown() {
        if (federation != null)
            federation.close();
    }

    @AfterClass
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Set<String> names = fusekiEndpoints.keySet();
        for (String name : names) {
            fusekiEndpoints.get(name).close();
            fusekiEndpoints.remove(name);
        }
        assertEquals(fusekiEndpoints.size(), 0); //no concurrency!
    }

    private static final @Nonnull List<Class<? extends DecompositionStrategy>>
            decomposerClasses = asList(EvenDecomposer.class, StandardDecomposer.class);
    private static final @Nonnull List<Class<? extends DecompositionStrategy>>
            fastDecomposerClasses = Collections.singletonList(StandardDecomposer.class);

    private static final @Nonnull List<TestModule> moduleList =
            OuterPlannerTest.classes.stream()
                .flatMap(op -> PlannerTest.plannerClasses.stream()
                    .flatMap(ip -> PlannerTest.joinOrderPlannerClasses.stream()
                        .flatMap(jo -> decomposerClasses.stream()
                            .flatMap(ds -> PerformanceListenerTest.classes.stream()
                                .flatMap(pl -> moduleProtoList.stream().map(p -> p.apply(op, ip, jo, ds, pl))))
                            )))
            .collect(toList());

    private static @Nonnull Object[][] prependModules(List<List<Object>> in) {
        List<List<Object>> rows = new ArrayList<>();
        for (TestModule module : moduleList) {
            for (List<Object> list : in) {
                if (((Setup)list.get(0)).requiresBindJoin() && module.cannotBindJoin())
                    continue; //skip
                if (((Setup)list.get(0)).requiresFastModule() && module.isSlowModule())
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


    public static @Nonnull List<List<Object>> transparencyJoinsData()
            throws IOException, SPARQLParseException {
        Class<FederationTest> myClass = FederationTest.class;
        String[] sparql = new String[4];
        for (int i = 0; i < 3; i++) {
            try (InputStream in = myClass.getResourceAsStream("transparency-query-"+i+".sparql")) {
                sparql[i] = IOUtils.toString(in, StandardCharsets.UTF_8);
            }
        }
        StdLit s267291791 = StdLit.fromUnescaped("267291791", xsdInt);
        StdLit s278614622 = StdLit.fromUnescaped("278614622", xsdInt);
        StdLit s70507179 = StdLit.fromUnescaped("70507179", xsdInt);
        StdLit s71407155 = StdLit.fromUnescaped("71407155", xsdInt);
        StdLit d20191202 = StdLit.fromUnescaped("2019-12-02", xsdDate);
        StdLit d20191017 = StdLit.fromUnescaped("2019-10-17", xsdDate);
        StdLit d20201202 = StdLit.fromUnescaped("2020-12-02", xsdDate);
        StdLit pregao = StdLit.fromUnescaped("Pregão - Registro de Preço", xsdString);

        return asList(
                /* match only against getContracts() */
                asList(new SetupTransparency(),
                        SPARQLQueryParser.strict().parse(sparql[0]),
                        newHashSet(MapSolution.build("id", s267291791),
                                   MapSolution.build("id", s278614622))),
                /* match only against getProcurements() */
                asList(new SetupTransparency(),
                       SPARQLQueryParser.strict().parse(sparql[1]),
                       newHashSet(MapSolution.builder()
                                       .put("id", s70507179)
                                       .put("startDate", d20191202)
                                       .put("endDate", d20201202)
                                       .put("modDescr", pregao).build(),
                                  MapSolution.builder()
                                       .put("id", s71407155)
                                       .put("startDate", d20191202)
                                       .put("endDate", d20201202)
                                       .put("modDescr", pregao).build())),
                /* Get procurement of a contract */
                asList(new SetupTransparency(),
                       SPARQLQueryParser.strict().parse(sparql[2]),
                       newHashSet(MapSolution.builder()
                                       .put("id", s70507179)
                                       .put("startDate", d20191202)
                                       .put("openDate", d20191017)
                                       .put("modDescr", pregao).build(),
                                  MapSolution.builder()
                                       .put("id", s71407155)
                                       .put("startDate", d20191202)
                                       .put("openDate", d20191017)
                                       .put("modDescr", pregao).build()))
//                /* Get procurement of a contract using linkedator link */
//                asList(new SetupTransparency(),
//                        SPARQLQueryParser.parse(sparql[3]),
//                        newHashSet(MapSolution.builder()
//                                        .put("id", s70507179)
//                                        .put("startDate", d20191202)
//                                        .put("openDate", d20191017).build(),
//                                MapSolution.builder()
//                                        .put("id", s71407155)
//                                        .put("startDate", d20191202)
//                                        .put("openDate", d20191017).build()))
                );
    }

    public static @Nonnull List<List<Object>> largeRDFBenchData() throws Exception {
        List<List<Object>> list = new ArrayList<>();
        for (String queryName : LargeRDFBenchSelfTest.QUERY_FILENAMES) {
            List<Object> row = new ArrayList<>();
            row.add(new SetupLargeRDFBench(0));
            row.add(LargeRDFBenchSelfTest.loadQuery(queryName));
            row.add(new HashSet<>(LargeRDFBenchSelfTest.loadResults(queryName).getCollection()));
            list.add(row);
        }
        return list;
    }

    @NotNull
    private static List<List<Object>> expandVariants(List<List<Object>> basic) {
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
        return withVariants;
    }

    @DataProvider
    public static Object[][] queryData() throws Exception {
        List<List<Object>> basic = new ArrayList<>();
        basic.addAll(singleTripleData());
        basic.addAll(singleEpQueryData());
        basic.addAll(crossEpJoinsData());
        basic.addAll(transparencyJoinsData());
        basic.addAll(largeRDFBenchData());

        List<List<Object>> withVariants = expandVariants(basic);
        return prependModules(withVariants);
    }

    @Test(dataProvider = "queryData")
    public void testQuery(@Nonnull Module module, @Nonnull Setup setup,
                          @Nonnull CQuery query,  @Nullable Set<Solution> expected) {
        Injector injector = Guice.createInjector(Modules.override(new SimpleExecutionModule())
                                                        .with(module));
        federation = injector.getInstance(Federation.class);
        setup.accept(federation, target().getUri().toString());

        Set<Solution> actual = new HashSet<>();
        federation.initAllSources(5, TimeUnit.MINUTES);
//        Stopwatch sw = Stopwatch.createStarted();
        PlanNode plan = federation.plan(query);
//        if (sw.elapsed(TimeUnit.MILLISECONDS) > 20)
//            System.out.printf("Slow plan: %f\n", sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
        PlannerTest.assertPlanAnswers(plan, query);

        // expected may be null telling us that we shouldn't execute, only check the plan
        if (expected != null) {
//            sw.reset().start();
            try (Results results = federation.query(query)) {
                results.forEachRemaining(actual::add);
            }
//            if (sw.elapsed(TimeUnit.MILLISECONDS) > 100)
//                System.out.printf("Slow execution: %f\n", sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
            assertEquals(actual, expected);
        }
    }

    @DataProvider
    public static Object[][] performanceListenerData() {
        // use less queries, since we only need to test the PerformanceListener
        List<List<Object>> basic = new ArrayList<>();
        basic.addAll(singleEpQueryData());
        basic.addAll(crossEpJoinsData());

        List<List<Object>> withVariants = expandVariants(basic);
        List<List<Object>> withListeners = new ArrayList<>();
        for (TestModule module : moduleList) {
            if (!module.hasStoringPerformanceListener())
                continue; //only use the listeners that store data
            for (List<Object> oldRow : withVariants) {
                if (((Setup)oldRow.get(0)).requiresBindJoin() && module.cannotBindJoin())
                    continue; //skip
                if (((Setup)oldRow.get(0)).requiresFastModule() && module.isSlowModule())
                    continue; //skip
                List<Object> newRow = new ArrayList<>(oldRow);
                newRow.add(0, module);
                withListeners.add(newRow);
            }
        }
        assertFalse(withListeners.isEmpty());
        return withListeners.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "performanceListenerData")
    public void testPerformanceListener(@Nonnull Module module, @Nonnull Setup setup,
                                        @Nonnull CQuery query,  @Nullable Set<Solution> expected) {
        Injector injector = Guice.createInjector(Modules.override(new SimpleExecutionModule())
                .with(module));
        federation = injector.getInstance(Federation.class);
        setup.accept(federation, target().getUri().toString());

        federation.initAllSources(5, TimeUnit.MINUTES);
        PlanNode plan = federation.plan(query);
        PerformanceListener perf = federation.getPerformanceListener();
        perf.sync();

        int sourcesCount = perf.getValue(Metrics.SOURCES_COUNT, -1);
        double initSourcesMs = perf.getValue(Metrics.INIT_SOURCES_MS, -1.0);
        double selectionMs = perf.getValue(Metrics.SELECTION_MS, -1.0);
        double agglutinationMs = perf.getValue(Metrics.AGGLUTINATION_MS, -1.0);
        double outPlanMs = perf.getValue(Metrics.OUT_PLAN_MS, -1.0);
        double planMs = perf.getValue(Metrics.PLAN_MS, -1.0);
        double optMs = perf.getValue(Metrics.OPT_MS, -1.0);

        assertTrue(sourcesCount > 0);
        assertTrue(initSourcesMs >= 0);
        assertTrue(selectionMs >= 0);
        assertTrue(agglutinationMs >= 0);
        assertTrue(outPlanMs >= 0);
        assertTrue(planMs >= 0);
        assertTrue(optMs >= 0);

        if (expected != null) {
            // consume just to make the machinery run
            Set<Solution> actual = new HashSet<>();
            try (Results results = federation.execute(query, plan)) {
                results.forEachRemaining(actual::add);
            }
            perf.sync();

            // values should not have changed
            assertEquals(perf.getValue(Metrics.SOURCES_COUNT), Integer.valueOf(sourcesCount));
            assertEquals(perf.getValue(Metrics.INIT_SOURCES_MS), initSourcesMs);
            assertEquals(perf.getValue(Metrics.SELECTION_MS), selectionMs);
            assertEquals(perf.getValue(Metrics.AGGLUTINATION_MS), agglutinationMs);
            assertEquals(perf.getValue(Metrics.OUT_PLAN_MS), outPlanMs);
            assertEquals(perf.getValue(Metrics.PLAN_MS), planMs);
            assertEquals(perf.getValue(Metrics.OPT_MS), optMs);

            assertEquals(actual.size(), expected.size()); //give an use to actual and expected
        }
    }

    @DataProvider
    public static Object[][] modulesData() {
        return moduleList.stream().map(m -> new Object[] {m}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "modulesData")
    public void testEmptyFederation(@Nonnull Module module) throws Exception {
        Injector injector = Guice.createInjector(Modules.override(new SimpleExecutionModule())
                .with(module));
        federation = injector.getInstance(Federation.class);
        Stopwatch sw = Stopwatch.createStarted();
        federation.initAllSources(20, TimeUnit.SECONDS);
        assertTrue(sw.elapsed(TimeUnit.SECONDS) < 15); // no reason to block

        CQuery cQuery = LargeRDFBenchSelfTest.loadQuery("S2");
        try (Results results = federation.query(cQuery)) {
            assertFalse(results.hasNext());
        }
        //pass if got here alive
    }

    @Test(dataProvider = "modulesData")
    public void testUninitializedSources(@Nonnull Module module) throws Exception {
        Injector injector = Guice.createInjector(Modules.override(new SimpleExecutionModule())
                .with(module));
        federation = injector.getInstance(Federation.class);

        ARQEndpoint dbpEp = forModel(loadData("DBPedia-Subset.nt"), "DBPedia-Subset");
        Source dbp = new Source(new SelectDescription(dbpEp), dbpEp);
        ARQEndpoint nytEp = forService("http://127.0.0.178:8897/sparql");
        Source nyt = new Source(new SelectDescription(nytEp), nytEp);
        federation.addSource(dbp);
        federation.addSource(nyt);

        federation.initAllSources(10, TimeUnit.SECONDS);
        CQuery query = LargeRDFBenchSelfTest.loadQuery("S2");
        try (Results results = federation.query(query)) {
            assertFalse(results.hasNext());
        }
        //pass if got here alive
    }

    private static final class SetupSimpleCNC implements Setup {
        int variantIdx;

        public SetupSimpleCNC() {
            this(0);
        }

        public SetupSimpleCNC(int idx) {
            assert idx >=0 && idx < 2;
            this.variantIdx = idx;
        }

        @Override
        public boolean requiresBindJoin() {
            return false;
        }
        @Override
        public boolean requiresFastModule() {
            return false;
        }

        @Override
        public @Nullable Setup nextVariant() {
            return variantIdx >= 1 ? null : new SetupSimpleCNC(variantIdx+1);
        }

        private @Nonnull TBoxReasoner createReasoner() {
            if (variantIdx == 0)
                return new TransitiveClosureTBoxReasoner();
            else if (variantIdx == 1)
                return OWLAPITBoxReasoner.hermit();
            throw new AssertionError("Unexpected variantIdx="+variantIdx);
        }

        @Override
        public void accept(Federation federation, String baseServiceUri) {
            ARQEndpoint data = createEndpoint("federation/cnc.ttl");
            TBoxReasoner reasoner = createReasoner();
            reasoner.load(new TBoxSpec().addResource(getClass(), "cnc-ontology.ttl"));
            SemanticSelectDescription matcher =
                    new SemanticSelectDescription(data, true, reasoner);
            federation.addSource(new Source(matcher, data));
        }

        @Override
        public String toString() {
            String reasoner = null;
            switch (variantIdx) {
                case 0: reasoner = "TransitiveClosureTBoxReasoner"; break;
                case 1: reasoner = "OWLAPITBoxReasoner.hermit()"; break;
                default: fail("Unexpected variantIdx="+variantIdx);
            }
            return "SetupSimpleCNC+"+reasoner;
        }
    }

    private static CQuery loadQuery(@Nonnull String filename) throws Exception {
        try (InputStream in = FederationTest.class.getResourceAsStream(filename)) {
            return SPARQLQueryParser.strict().parse(in);
        }
    }

    @DataProvider
    public static @Nonnull Object[][] reasoningQueriesData() throws Exception {
        return Stream.of(
                asList(new AbstractModule() {}, new SetupSimpleCNC(),
                       loadQuery("cnc-qry-1.sparql"), Sets.newHashSet(
                               MapSolution.build(x, fromJena(createTypedLiteral("11"))),
                               MapSolution.build(x, fromJena(createTypedLiteral("21"))),
                               MapSolution.build(x, fromJena(createTypedLiteral("12"))),
                               MapSolution.build(x, fromJena(createTypedLiteral("22")))
                        ))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "reasoningQueriesData")
    public void testReasoningQueries(@Nonnull Module module, @Nonnull Setup setup,
                                     @Nonnull CQuery query, @Nonnull Set<Solution> expected) {
        Module effectiveModule = Modules.override(new SimpleFederationModule()).with(module);
        Federation federation = Guice.createInjector(effectiveModule).getInstance(Federation.class);
        setup.accept(federation, target().getUri().toString());
        federation.initAllSources(5, TimeUnit.MINUTES);
        PlanNode plan = federation.plan(query);
        PlannerTest.assertPlanAnswers(plan, query);

        Set<Solution> actual = new HashSet<>();
        Results results = federation.execute(query, plan);
        results.forEachRemainingThenClose(actual::add);
        assertEquals(actual, expected);
    }
}