package br.ufsc.lapesd.freqel.federation;

import br.ufsc.lapesd.freqel.BSBMSelfTest;
import br.ufsc.lapesd.freqel.LargeRDFBenchSelfTest;
import br.ufsc.lapesd.freqel.ResultsAssert;
import br.ufsc.lapesd.freqel.TestContext;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.algebra.leaf.QueryOp;
import br.ufsc.lapesd.freqel.algebra.util.TreeUtils;
import br.ufsc.lapesd.freqel.cardinality.impl.GeneralSelectivityHeuristic;
import br.ufsc.lapesd.freqel.cardinality.impl.NoCardinalityEnsemble;
import br.ufsc.lapesd.freqel.cardinality.impl.QuickSelectivityHeuristic;
import br.ufsc.lapesd.freqel.cardinality.impl.WorstCaseCardinalityEnsemble;
import br.ufsc.lapesd.freqel.description.AskDescription;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.description.SelectDescription;
import br.ufsc.lapesd.freqel.description.molecules.Molecule;
import br.ufsc.lapesd.freqel.description.semantic.AlternativesSemanticSelectDescription;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.EvenAgglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.ParallelStandardAgglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.agglutinator.StandardAgglutinator;
import br.ufsc.lapesd.freqel.federation.decomp.match.ParallelSourcesListMatchingStrategy;
import br.ufsc.lapesd.freqel.federation.decomp.match.SourcesListMatchingStrategy;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.DefaultHashJoinOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.FixedBindJoinOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.FixedHashJoinOpExecutor;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.freqel.federation.execution.tree.impl.joins.hash.ParallelInMemoryHashJoinResults;
import br.ufsc.lapesd.freqel.federation.inject.dagger.DaggerTestComponent;
import br.ufsc.lapesd.freqel.federation.inject.dagger.TestComponent;
import br.ufsc.lapesd.freqel.federation.performance.NoOpPerformanceListener;
import br.ufsc.lapesd.freqel.federation.performance.metrics.Metrics;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.ArbitraryJoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.GreedyJoinOrderPlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.JoinPathsConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.BitsetConjunctivePlanner;
import br.ufsc.lapesd.freqel.federation.planner.conjunctive.bitset.BitsetConjunctivePlannerDispatcher;
import br.ufsc.lapesd.freqel.federation.planner.equiv.DefaultEquivCleaner;
import br.ufsc.lapesd.freqel.federation.planner.equiv.NoEquivCleaner;
import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.linkedator.Linkedator;
import br.ufsc.lapesd.freqel.linkedator.LinkedatorResult;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Res;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.owlapi.reason.tbox.OWLAPITBoxMaterializer;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.TPEndpointTest;
import br.ufsc.lapesd.freqel.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.CQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.decorators.EndpointDecorators;
import br.ufsc.lapesd.freqel.query.endpoint.impl.SPARQLClient;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParseException;
import br.ufsc.lapesd.freqel.query.parse.SPARQLParser;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import br.ufsc.lapesd.freqel.reason.tbox.EmptyTBox;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxMaterializer;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.freqel.reason.tbox.TransitiveClosureTBoxMaterializer;
import br.ufsc.lapesd.freqel.util.ModelMessageBodyWriter;
import br.ufsc.lapesd.freqel.webapis.TransparencyService;
import br.ufsc.lapesd.freqel.webapis.TransparencyServiceTestContext;
import br.ufsc.lapesd.freqel.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.freqel.webapis.description.APIMolecule;
import br.ufsc.lapesd.freqel.webapis.requests.impl.UriTemplateExecutor;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
import java.util.function.Consumer;
import java.util.stream.Stream;

import static br.ufsc.lapesd.freqel.PlanAssert.assertPlanAnswers;
import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.streamPreOrder;
import static br.ufsc.lapesd.freqel.federation.FreqelConfig.Key.*;
import static br.ufsc.lapesd.freqel.jena.JenaWrappers.*;
import static br.ufsc.lapesd.freqel.jena.query.ARQEndpoint.forModel;
import static br.ufsc.lapesd.freqel.jena.query.ARQEndpoint.forService;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static br.ufsc.lapesd.freqel.webapis.TransparencyService.*;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.jena.rdf.model.ResourceFactory.*;
import static org.testng.Assert.*;

@SuppressWarnings("SameParameterValue")
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

    private static @Nonnull ARQEndpoint createEndpoint(@Nonnull String filename) {
        Model m = ModelFactory.createDefaultModel();
        RDFDataMgr.read(m, new TestContext(){}.open(filename), Lang.TTL);
        return forModel(m);
    }

    private static @Nonnull TPEndpoint createWebAPISource(@Nonnull Molecule molecule,
                                                          @Nonnull String base,
                                                          @Nonnull String file) {
        UriTemplate uriTemplate = new UriTemplate(base+"/describe/"+file+"{?uri}");
        UriTemplateExecutor executor = new UriTemplateExecutor(uriTemplate);
        Map<String, String> atom2input = new HashMap<>();
        atom2input.put(molecule.getCore().getName(), "uri");
        APIMolecule apiMolecule = new APIMolecule(molecule, executor, atom2input);
        return new WebAPICQEndpoint(apiMolecule);

    }

    private static @Nonnull TPEndpoint
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
        return new WebAPICQEndpoint(apiMolecule);
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
            federation.addSource(ep.setDescription(new SelectDescription(ep)));
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
            federation.addSource(ep.setDescription(new SelectDescription(ep)));
            ep = createEndpoint("rdf-2.nt");
            federation.addSource(ep.setDescription(new AskDescription(ep)));
        }
        @Override
        public boolean requiresBindJoin() { return false; }
        @Override
        public boolean requiresFastModule() { return false; }
        @Override
        public String toString() { return "SetupTwoEps";}
    }

    private static final class SetupBookShop implements Setup {
        private final int variantIdx;

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
            federation.addSource(authors.setDescription(new SelectDescription(authors)));
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
                federation.addSource(books.setDescription(new SelectDescription(books)));
            }
            ARQEndpoint genres = createEndpoint("genres.nt");
            federation.addSource(genres.setDescription(new SelectDescription(genres)));
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
                federation.addSource(getContractsClient(target));
                federation.addSource(getContractByIdClient(target));
                federation.addSource(getProcurementsClient(target));
                federation.addSource(getProcurementsByIdClient(target));
                federation.addSource(getProcurementByNumberClient(target));
                federation.addSource(getOrgaosSiafiClient(target));
                ARQEndpoint ep = createEndpoint("modalidades.ttl");
                federation.addSource(ep.setDescription(new SelectDescription(ep)));
                //some completely unrelated endpoints...
                ep = createEndpoint("books.nt");
                federation.addSource(ep.setDescription(new SelectDescription(ep)));
                ep = createEndpoint("genres.nt");
                federation.addSource(ep.setDescription(new SelectDescription(ep)));

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

        static @Nonnull TPEndpoint wrap(@Nonnull Model model, @Nonnull String endpointName,
                                        @Nonnull String variantName) {
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
                description = new AlternativesSemanticSelectDescription(ep, true, new EmptyTBox());
            assert description != null;
            ((AbstractTPEndpoint)ep).setDescription(description);

            if (!variantName.contains("SPARQLClient"))
                ep = EndpointDecorators.uncloseable(ep);
            return ep;
        }

        @Override
        public void accept(Federation federation, String base) {
            String variantName = variantNames[variantIdx];

            ClassLoader cl = Thread.currentThread().getContextClassLoader();
            Model union = ModelFactory.createDefaultModel();
            for (String filename : LargeRDFBenchSelfTest.DATA_FILENAMES) {
                try (InputStream stream = cl.getResourceAsStream(LargeRDFBenchSelfTest.RESOURCE_DIR + "/data/" + filename)) {
                    assertNotNull(stream);
                    if (variantName.contains("union")) {
                        RDFDataMgr.read(union, stream, Lang.NT);
                    } else {
                        Model model = ModelFactory.createDefaultModel();
                        RDFDataMgr.read(model, stream, Lang.NT);
                        federation.addSource(wrap(model, filename, variantName));
                    }
                } catch (IOException e) {
                    fail("Unexpected exception", e);
                }
            }
            if (variantName.contains("union"))
                federation.addSource(wrap(union, "LargeRDFBench-union", variantName));
        }

        @Override
        public @Nonnull String toString() {
            return "SetupLargeRDFBench["+variantNames[variantIdx]+"]";
        }
    }

    private final static class SetupBSBM implements Setup {
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

        public SetupBSBM(int variantIdx) {
            this.variantIdx = variantIdx;
        }

        @Override
        public @Nullable Setup nextVariant() {
            return variantIdx >= variantNames.length-1
                    ? null : new SetupBSBM(variantIdx+1);
        }

        @Override
        public boolean requiresBindJoin() {
            return false;
        }

        @Override
        public boolean requiresFastModule() {
            return true;
        }

        @Override
        public void accept(@Nonnull Federation federation, @Nullable String ignored) {
            String variant = variantNames[variantIdx];
            if (variant.contains("union")) {
                Model model = BSBMSelfTest.allData();
                TPEndpoint src = SetupLargeRDFBench.wrap(model, "BSBM", variant);
                federation.addSource(src);
            } else {
                for (String filename : BSBMSelfTest.DATA_FILENAMES) {
                    Model model = null;
                    try {
                        model = BSBMSelfTest.loadData(filename);
                    } catch (IOException e) {
                        fail("Could not load BSBM dataset "+filename, e);
                    }
                    TPEndpoint src = SetupLargeRDFBench.wrap(model, filename, variant);
                    federation.addSource(src);
                }
            }
        }

        @Override
        public String toString() {
            return "SetupBSBM["+variantNames[variantIdx]+"]";
        }
    }

    private static class FederationFactory {
        private final FreqelConfig defaultConfig = FreqelConfig.createDefault();
        private final FreqelConfig config = FreqelConfig.createDefault();
        private final Map<FreqelConfig.Key, Object> explicit = new HashMap<>();

        public boolean onlyHashJoin() {
            HashSet<String> hashExecutors = newHashSet(DefaultHashJoinOpExecutor.class.getName(),
                                                       FixedHashJoinOpExecutor.class.getName());
            return hashExecutors.contains(config.get(JOIN_OP_EXECUTOR, String.class));
        }
        public boolean isDefault() {
            return explicit.isEmpty();
        }
        public boolean isFast() {
            if (isDefault())
                return true;
            String conjPlanner = BitsetConjunctivePlannerDispatcher.class.getName();
            String matcher = SourcesListMatchingStrategy.class.getName();
            String agglutinator = StandardAgglutinator.class.getName();
            String equivCleaner = NoEquivCleaner.class.getName();
            return config.get(CONJUNCTIVE_PLANNER, String.class).equals(conjPlanner)
                    && config.get(MATCHING, String.class).equals(matcher)
                    && config.get(AGGLUTINATOR, String.class).equals(agglutinator)
                    && config.get(EQUIV_CLEANER, String.class).equals(equivCleaner);
        }
        public boolean hasStoringPerformanceListener() {
            return !config.get(FreqelConfig.Key.PERFORMANCE_LISTENER, String.class)
                    .equals(NoOpPerformanceListener.class.getName());
        }

        public FederationFactory assertEquals(@Nonnull FreqelConfig.Key key,
                                              @Nullable Object expected) {
            if (expected instanceof Class)
                expected = ((Class<?>) expected).getName();
            Assert.assertEquals(config.get(key).toString(), Objects.toString(expected));
            return this;
        }

        public FederationFactory set(@Nonnull FreqelConfig.Key key, @Nonnull Object value) {
            if (value instanceof Class)
                value = ((Class<?>) value).getName();
            if (Objects.equals(Objects.toString(defaultConfig.get(key)), Objects.toString(value))) {
                explicit.remove(key);
            } else {
                config.set(key, value);
                explicit.put(key, value);
            }
            return this;
        }

        public @Nonnull Federation create() {
            TestComponent.Builder builder = DaggerTestComponent.builder();
            builder.overrideFreqelConfig(config);
            return builder.build().federation();
        }

        @Override public @Nonnull String toString() {
            StringBuilder b = new StringBuilder("[");
            for (Map.Entry<FreqelConfig.Key, Object> e : explicit.entrySet()) {
                if (e.getValue() instanceof Class)
                    b.append(((Class<?>) e.getValue()).getSimpleName()).append(", ");
                else if (e.getValue().toString().startsWith("br.ufsc.lapesd"))
                    b.append(e.getValue().toString().replaceAll(".*\\.", "")).append(", ");
                else
                    b.append(e.getKey()).append('=').append(e.getValue()).append(", ");
            }
            if (!explicit.isEmpty())
                b.setLength(b.length()-2);
            return b.append(']').toString();
        }
    }

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

    private static final @Nonnull List<Consumer<FederationFactory>> nonCartesianVariants = asList(
            f -> {
                f.assertEquals(CARDINALITY_ENSEMBLE, WorstCaseCardinalityEnsemble.class);
                f.set(CARDINALITY_ENSEMBLE, NoCardinalityEnsemble.class);
            },
            f -> f.set(CARDINALITY_HEURISTICS, singletonList(GeneralSelectivityHeuristic.class)),
            f -> f.set(CARDINALITY_HEURISTICS, singletonList(QuickSelectivityHeuristic.class)),
            f -> {
                f.assertEquals(ESTIMATE_QUERY_LOCAL, true);
                f.assertEquals(ESTIMATE_ASK_REMOTE, false);
                f.assertEquals(ESTIMATE_LIMIT, 100);
                f.set(RESULTS_EXECUTOR_CONCURRENCY_FACTOR, 0); //sequential
            },
            f -> f.set(RESULTS_EXECUTOR_CONCURRENCY_FACTOR, -1), //unbound
            f -> {
                f.set(JOIN_OP_EXECUTOR, FixedHashJoinOpExecutor.class);
                f.set(HASH_JOIN_RESULTS_FACTORY, InMemoryHashJoinResults.Factory.class);
            },
            f -> {
                f.set(JOIN_OP_EXECUTOR, FixedHashJoinOpExecutor.class);
                f.set(HASH_JOIN_RESULTS_FACTORY, ParallelInMemoryHashJoinResults.Factory.class);
            },
            f -> {
                f.set(JOIN_OP_EXECUTOR, FixedBindJoinOpExecutor.class);
                f.set(BIND_JOIN_RESULTS_FACTORY, SimpleBindJoinResults.Factory.class);
            }
    );
    private static final @Nonnull List<FederationFactory> factoryList;
    static  {
        List<FederationFactory> factories = new ArrayList<>();
        List<List<? extends Class<?>>> lists = asList(
                asList(JoinPathsConjunctivePlanner.class, BitsetConjunctivePlanner.class,
                        BitsetConjunctivePlannerDispatcher.class),                     // 0
                asList(ArbitraryJoinOrderPlanner.class, GreedyJoinOrderPlanner.class), // 1
                asList(NoEquivCleaner.class, DefaultEquivCleaner.class),               // 2
                asList(SourcesListMatchingStrategy.class,
                       ParallelSourcesListMatchingStrategy.class),                     // 3
                asList(EvenAgglutinator.class,
                       StandardAgglutinator.class,
                       ParallelStandardAgglutinator.class),                            // 4
                PerformanceListenerTest.classes                                        // 5
        );
        for (List<Class<?>> selection : Lists.cartesianProduct(lists)) {
            for (Consumer<FederationFactory> variant : nonCartesianVariants) {
                FederationFactory factory = new FederationFactory();
                factory.set(CONJUNCTIVE_PLANNER, selection.get(0));
                factory.set(JOIN_ORDER_PLANNER, selection.get(1));
                factory.set(EQUIV_CLEANER, selection.get(2));
                factory.set(MATCHING, selection.get(3));
                factory.set(AGGLUTINATOR, selection.get(4));
                factory.set(PERFORMANCE_LISTENER, selection.get(5));
                variant.accept(factory);
                factories.add(factory);
            }
        }
        factoryList = factories;
    }


    private static @Nonnull Object[][] prependModules(List<List<Object>> in) {
        List<List<Object>> rows = new ArrayList<>();
        for (FederationFactory factory : factoryList) {
            for (List<Object> list : in) {
                if (((Setup)list.get(0)).requiresBindJoin() && factory.onlyHashJoin())
                    continue; //skip
                if (((Setup)list.get(0)).requiresFastModule() && !factory.isFast())
                    continue; //skip
                ArrayList<Object> row = new ArrayList<>();
                row.add(factory);
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
                        createQuery(x, author, y, y, nameEx, author1, Projection.of("x")),
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
                        createQuery(x, author, y,
                                    x, genre, z,
                                    y, nameEx, author1,
                                    z, genreName, genre1, Projection.of("x")),
                        newHashSet(MapSolution.build(x, ex("books/1")))),
                asList(new SetupBookShop(0),
                        createQuery(x, author, y,
                                    x, genre, z,
                                    y, nameEx, author1,
                                    z, genreName, genre1, Projection.of("y")),
                        newHashSet(MapSolution.build(y, ex("authors/1")))),
                asList(new SetupBookShop(0),
                       CQuery.from(asList(new Triple(x, title, title1),
                                          new Triple(x, genre, y),
                                          new Triple(y, genreName, z))),
                       newHashSet(MapSolution.builder().put(x, ex("books/1"))
                                                       .put(y, ex("genres/1"))
                                                       .put(z, genre1).build())),
                asList(new SetupBookShop(0),
                       createQuery(x, title, title1,
                                   x, genre, y,
                                   y, genreName, z, Projection.of("x")),
                       newHashSet(MapSolution.build(x, ex("books/1"))))
        );
    }


    public static @Nonnull List<List<Object>> transparencyJoinsData()
            throws IOException, SPARQLParseException {
        String[] sparql = new String[4];
        for (int i = 0; i < 3; i++) {
            String queryFile = "federation/transparency-query-" + i + ".sparql";
            try (InputStream in = new TestContext(){}.open(queryFile)) {
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
                        SPARQLParser.strict().parse(sparql[0]),
                        newHashSet(MapSolution.build("id", s267291791),
                                   MapSolution.build("id", s278614622))),
                /* match only against getProcurements() */
                asList(new SetupTransparency(),
                       SPARQLParser.strict().parse(sparql[1]),
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
                       SPARQLParser.strict().parse(sparql[2]),
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

    public static @Nonnull List<List<Object>> bsbmData() throws Exception {
        List<List<Object>> list = new ArrayList<>();
        for (String queryName : BSBMSelfTest.QUERY_FILENAMES) {
            List<Object> row = new ArrayList<>();
            row.add(new SetupBSBM(0));
            row.add(BSBMSelfTest.loadQuery(queryName));
            row.add(new HashSet<>(BSBMSelfTest.loadResults(queryName).getCollection()));
            list.add(row);
        }
        return list;
    }

    @Nonnull
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
        basic.addAll(bsbmData());

        List<List<Object>> withVariants = expandVariants(basic);
        return prependModules(withVariants);
    }

    @Test(dataProvider = "queryData")
    public void testQuery(@Nonnull FederationFactory factory, @Nonnull Setup setup,
                          @Nonnull Object queryObject,  @Nullable Set<Solution> expected) {
        federation = factory.create();
        setup.accept(federation, target().getUri().toString());

        federation.initAllSources(5, TimeUnit.MINUTES);
//        Stopwatch sw = Stopwatch.createStarted();
        Op query = queryObject instanceof Op ? (Op)queryObject
                                             : new QueryOp((CQuery) queryObject);
        Op oldQuery = TreeUtils.deepCopy(query);
        int oldQueryHash = query.hashCode();
        assertEquals(oldQuery, query);
        assertEquals(oldQuery.hashCode(), oldQueryHash);

        Op plan = federation.plan(query);
        assertEquals(query.hashCode(), oldQueryHash); //should not be modified
        assertEquals(query, oldQuery);
//        if (sw.elapsed(TimeUnit.MILLISECONDS) > 20)
//            System.out.printf("Slow plan: %f\n", sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
        assertPlanAnswers(plan, query);

        // plan always works
        for (int i = 0; i < 4; i++)
            assertPlanAnswers(federation.plan(query), query);

        boolean hasInputs = streamPreOrder(plan).anyMatch(o -> !o.getInputVars().isEmpty());
        // expected may be null telling us that we shouldn't execute, only check the plan
        if (expected != null && (!hasInputs || !factory.onlyHashJoin()))
            ResultsAssert.assertExpectedResults(federation.query(query), expected);
    }


    @DataProvider
    public static Object[][] performanceListenerData() {
        // use less queries, since we only need to test the PerformanceListener
        List<List<Object>> basic = new ArrayList<>();
        basic.addAll(singleEpQueryData());
        basic.addAll(crossEpJoinsData());

        List<List<Object>> withVariants = expandVariants(basic);
        List<List<Object>> withListeners = new ArrayList<>();
        for (FederationFactory factory : factoryList) {
            if (!factory.hasStoringPerformanceListener())
                continue; //only use the listeners that store data
            for (List<Object> oldRow : withVariants) {
                if (((Setup)oldRow.get(0)).requiresBindJoin() && factory.onlyHashJoin())
                    continue; //skip
                if (((Setup)oldRow.get(0)).requiresFastModule() && !factory.isFast())
                    continue; //skip
                List<Object> newRow = new ArrayList<>(oldRow);
                newRow.add(0, factory);
                withListeners.add(newRow);
            }
        }
        assertFalse(withListeners.isEmpty());
        return withListeners.stream().map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "performanceListenerData")
    public void testPerformanceListener(@Nonnull FederationFactory factory, @Nonnull Setup setup,
                                        @Nonnull CQuery query,  @Nullable Set<Solution> expected) {
        federation = factory.create();
        setup.accept(federation, target().getUri().toString());

        federation.initAllSources(5, TimeUnit.MINUTES);
        Op plan = federation.plan(query);
        assertPlanAnswers(plan, query);
        PerformanceListener perf = federation.getPerformanceListener();
        perf.sync();

        int sourcesCount = perf.getValue(Metrics.SOURCES_COUNT, -1);
        double initSourcesMs = perf.getValue(Metrics.INIT_SOURCES_MS, -1.0);
        double selectionMs = perf.getValue(Metrics.SELECTION_MS, -1.0);
        double agglutinationMs = perf.getValue(Metrics.AGGLUTINATION_MS, -1.0);
        double outPlanMs = perf.getValue(Metrics.PRE_PLAN_MS, -1.0);
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
            ResultsAssert.assertExpectedResults(federation.execute(plan), expected);
            perf.sync();

            // values should not have changed
            assertEquals(perf.getValue(Metrics.SOURCES_COUNT), Integer.valueOf(sourcesCount));
            assertEquals(perf.getValue(Metrics.INIT_SOURCES_MS), initSourcesMs);
            assertEquals(perf.getValue(Metrics.SELECTION_MS), selectionMs);
            assertEquals(perf.getValue(Metrics.AGGLUTINATION_MS), agglutinationMs);
            assertEquals(perf.getValue(Metrics.PRE_PLAN_MS), outPlanMs);
            assertEquals(perf.getValue(Metrics.PLAN_MS), planMs);
            assertEquals(perf.getValue(Metrics.OPT_MS), optMs);
        }
    }

    @DataProvider
    public static Object[][] componentFactoryData() {
        return factoryList.stream().map(m -> new Object[] {m}).toArray(Object[][]::new);
    }

    @Test(dataProvider = "componentFactoryData")
    public void testEmptyFederation(@Nonnull FederationFactory factory) throws Exception {
        federation = factory.create();
        Stopwatch sw = Stopwatch.createStarted();
        federation.initAllSources(20, TimeUnit.SECONDS);
        assertTrue(sw.elapsed(TimeUnit.SECONDS) < 15); // no reason to block

        Op cQuery = LargeRDFBenchSelfTest.loadQuery("S2");
        try (Results results = federation.query(cQuery)) {
            assertFalse(results.hasNext());
        }
        //pass if got here alive
    }

    @Test(dataProvider = "componentFactoryData")
    public void testUninitializedSources(@Nonnull FederationFactory factory) throws Exception {
        federation = factory.create();
        ARQEndpoint dbp = forModel(LargeRDFBenchSelfTest.loadData("DBPedia-Subset.nt"), "DBPedia-Subset");
        dbp.setDescription(new SelectDescription(dbp));
        ARQEndpoint nyt = forService("http://127.0.0.178:8897/sparql");
        nyt.setDescription(new SelectDescription(nyt));
        federation.addSource(dbp);
        federation.addSource(nyt);

        federation.initAllSources(10, TimeUnit.SECONDS);
        Op query = LargeRDFBenchSelfTest.loadQuery("S2");
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

        private @Nonnull TBoxMaterializer createReasoner() {
            if (variantIdx == 0)
                return new TransitiveClosureTBoxMaterializer();
            else if (variantIdx == 1)
                return OWLAPITBoxMaterializer.hermit();
            throw new AssertionError("Unexpected variantIdx="+variantIdx);
        }

        @Override
        public void accept(Federation federation, String baseServiceUri) {
            ARQEndpoint data = createEndpoint("federation/cnc.ttl");
            TBoxMaterializer reasoner = createReasoner();
            reasoner.load(new TBoxSpec().addResource(getClass(), "cnc-ontology.ttl"));
            AlternativesSemanticSelectDescription matcher =
                    new AlternativesSemanticSelectDescription(data, true, reasoner);
            federation.addSource(data.setDescription(matcher));
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

    private static Op loadQuery(@Nonnull String filename) throws Exception {
        try (InputStream in = new TestContext(){}.open(filename)) {
            return SPARQLParser.strict().parse(in);
        }
    }

    @DataProvider
    public static @Nonnull Object[][] reasoningQueriesData() throws Exception {
        return Stream.of(
                asList(new FederationFactory() {}, new SetupSimpleCNC(),
                       loadQuery("cnc-qry-1.sparql"), Sets.newHashSet(
                               MapSolution.build(x, fromJena(createTypedLiteral("11"))),
                               MapSolution.build(x, fromJena(createTypedLiteral("21"))),
                               MapSolution.build(x, fromJena(createTypedLiteral("12"))),
                               MapSolution.build(x, fromJena(createTypedLiteral("22")))
                        ))
        ).map(List::toArray).toArray(Object[][]::new);
    }

    @Test(dataProvider = "reasoningQueriesData")
    public void testReasoningQueries(@Nonnull FederationFactory factory, @Nonnull Setup setup,
                                     @Nonnull Op query, @Nonnull Set<Solution> expected) {
        Federation federation = factory.create();
        setup.accept(federation, target().getUri().toString());
        federation.initAllSources(5, TimeUnit.MINUTES);
        Op plan = federation.plan(query);
        assertPlanAnswers(plan, query);
        ResultsAssert.assertExpectedResults(federation.execute(plan), expected);
    }
}