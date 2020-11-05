package br.ufsc.lapesd.riefederator.federation.execution;

import br.ufsc.lapesd.riefederator.ResultsAssert;
import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.CartesianOp;
import br.ufsc.lapesd.riefederator.algebra.inner.JoinOp;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.federation.execution.tree.*;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.LazyCartesianOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleEmptyOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleQueryOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.DefaultHashJoinOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.DefaultJoinOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.FixedBindJoinOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.FixedHashJoinOpExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.HashJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.ParallelInMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.jena.model.term.JenaRes;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.ResultsExecutor;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.query.results.impl.SequentialResultsExecutor;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.ModelMessageBodyWriter;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.UriTemplateExecutor;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Module;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.uri.UriTemplate;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.*;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

public class PlanExecutorTest extends JerseyTestNg.ContainerPerClassTest implements TestContext {
    public static final @Nonnull StdLit bob = StdLit.fromUnescaped("bob", "en");
    public static final @Nonnull StdLit beto = StdLit.fromUnescaped("beto", "pt");

    public static @Nonnull StdURI ex(@Nonnull String local) {
        return new StdURI("http://example.org/"+local);
    }

    /* ~~~ Modules ~~~ */

    public static class SimpleModule extends AbstractModule {
        private final boolean canBindJoin;
        public SimpleModule() {
            this(false);
        }
        public SimpleModule(boolean canBindJoin) {
            this.canBindJoin = canBindJoin;
        }
        public boolean canBindJoin() {return canBindJoin;}

        @Override
        protected void configure() {
            bind(ResultsExecutor.class).to(SequentialResultsExecutor.class);
            bind(QueryOpExecutor.class).to(SimpleQueryOpExecutor.class);
            bind(DQueryOpExecutor.class).to(SimpleQueryOpExecutor.class);
            bind(UnionOpExecutor.class).to(SimpleQueryOpExecutor.class);
            bind(SPARQLValuesTemplateOpExecutor.class).to(SimpleQueryOpExecutor.class);
            bind(CartesianOpExecutor.class).to(LazyCartesianOpExecutor.class);
            bind(EmptyOpExecutor.class).toInstance(SimpleEmptyOpExecutor.INSTANCE);
            bind(PlanExecutor.class).to(InjectedExecutor.class);
        }
    }

    public static final @Nonnull List<SimpleModule> modules = asList(
            new SimpleModule() {
                @Override
                protected void configure() { //Simple* implementations
                    super.configure();
                    bind(JoinOpExecutor.class).to(FixedHashJoinOpExecutor.class);
                    bind(HashJoinResultsFactory.class).toInstance(InMemoryHashJoinResults.FACTORY);
                }
                @Override
                public @Nonnull String toString() {
                    return "Fixed InMemoryHashJoinResults";
                }
            },
            new SimpleModule() {
                @Override
                protected void configure() {
                    super.configure();
                    bind(JoinOpExecutor.class).to(FixedHashJoinOpExecutor.class);
                    bind(HashJoinResultsFactory.class).toInstance(ParallelInMemoryHashJoinResults.FACTORY);
                }
                @Override
                public @Nonnull String toString() {
                    return "Fixed ParallelInMemoryHashJoinResults";
                }
            },
            new SimpleModule() {
                @Override
                protected void configure() {
                    super.configure();
                    bind(JoinOpExecutor.class).to(DefaultHashJoinOpExecutor.class);
                }
                @Override
                public @Nonnull String toString() {
                    return "HashJoinNodeExecutor";
                }
            },
            new SimpleModule(true) {
                @Override
                protected void configure() {
                    super.configure();
                    bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
                    bind(JoinOpExecutor.class).to(DefaultJoinOpExecutor.class);
                }
                @Override
                public @Nonnull String toString() {
                    return "DefaultJoinNodeExecutor";
                }
            },
            new SimpleModule(true) {
                @Override
                protected void configure() {
                    super.configure();
                    bind(JoinOpExecutor.class).to(FixedBindJoinOpExecutor.class);
                    bind(BindJoinResultsFactory.class).to(SimpleBindJoinResults.Factory.class);
                }
                @Override
                public @Nonnull String toString() {
                    return "Fixed SimpleBindJoinResults";
                }
            }
    );

    @DataProvider
    public static@Nonnull Object[][] modulesData() {
        return modules.stream().map(m -> new Object[]{m}).toArray(Object[][]::new);
    }

    @DataProvider
    public static@Nonnull Object[][] bindJoinModulesData() {
        return modules.stream().filter(SimpleModule::canBindJoin)
                .map(m -> new Object[]{m}).toArray(Object[][]::new);
    }

    /* ~~~ service ~~~ */

    @Path("/")
    public static class Service {
        @GET
        @Path("type")
        public @Nonnull Model type(@QueryParam("uri") String uri) {
            Model src = ModelFactory.createDefaultModel();
            RDFDataMgr.read(src, getClass().getResourceAsStream("../../rdf-joins.nt"), Lang.TTL);
            Model out = ModelFactory.createDefaultModel();
            ARQEndpoint ep = ARQEndpoint.forModel(src);
            JenaRes type = fromJena(RDF.type);
            try (Results r = ep.query(CQuery.from(new Triple(new StdURI(uri), type, x)))) {
                Resource subj = ResourceFactory.createResource(uri);
                while (r.hasNext())
                    out.add(subj, RDF.type, toJena(r.next().get("x")));
            }
            return out;
        }

        @GET
        @Path("getPO")
        public @Nonnull Model getPO(@QueryParam("uri") String uri) {
            Model src = ModelFactory.createDefaultModel();
            RDFDataMgr.read(src, getClass().getResourceAsStream("../../rdf-joins.nt"), Lang.TTL);
            Model out = ModelFactory.createDefaultModel();
            ARQEndpoint ep = ARQEndpoint.forModel(src);
            try (Results r = ep.query(CQuery.from(new Triple(new StdURI(uri), x, y)))) {
                Resource subj = ResourceFactory.createResource(uri);
                while (r.hasNext()) {
                    Solution solution = r.next();
                    out.add(subj, toJenaProperty(solution.get("x")), toJena(solution.get("y")));
                }
            }
            return out;
        }
    }

    /* ~~~ Endpoints ~~~ */

    public ARQEndpoint ep, joinsEp;
    public WebAPICQEndpoint typeEp;
    public WebAPICQEndpoint poEP;
    public Atom typedThing, poThing, typeAtom, knowsAtom, consumerAtom, clientAtom, costumerAtom;

    @BeforeMethod
    public void methodSetUp() {
        Lang ttl = Lang.TTL;
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, getClass().getResourceAsStream("../../rdf-1.nt"), ttl);
        ep = ARQEndpoint.forModel(model);

        model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, getClass().getResourceAsStream("../../rdf-joins.nt"), ttl);
        joinsEp = ARQEndpoint.forModel(model);

        Molecule typeMolecule = Molecule.builder("Thing")
                .out(type, typeAtom = Molecule.builder("type").buildAtom())
                .exclusive().build();
        typedThing = typeMolecule.getCore();
        Map<String, String> atom2input = new HashMap<>();
        atom2input.put("Thing", "uri");
        UriTemplate template = new UriTemplate(target().getUri() + "type{?uri}");
        UriTemplateExecutor templateExecutor = new UriTemplateExecutor(template);
        typeEp = new WebAPICQEndpoint(new APIMolecule(typeMolecule, templateExecutor, atom2input));

        Molecule poMolecule = Molecule.builder("Thing")
                .out(type, typeAtom)
                .out(knows, knowsAtom = Molecule.builder("known").buildAtom())
                .out(ex("hasConsumer"), consumerAtom = Molecule.builder("Consumer").buildAtom())
                .out(ex("hasClient"), clientAtom = Molecule.builder("Client").buildAtom())
                .out(ex("soldTo"), costumerAtom = Molecule.builder("Costumer").buildAtom())
                .exclusive().build();
        poThing = poMolecule.getCore();
        atom2input.clear();
        atom2input.put("Thing", "uri");
        template = new UriTemplate(target().getUri()+"getPO{?uri}");
        templateExecutor = new UriTemplateExecutor(template);
        poEP = new WebAPICQEndpoint(new APIMolecule(poMolecule, templateExecutor, atom2input));
    }

    @Override
    protected Application configure() {
        return new ResourceConfig().register(ModelMessageBodyWriter.class).register(Service.class);
    }

    /* ~~~ test cases ~~~ */

    @Test
    public void selfTestType() throws UnsupportedEncodingException {
        Resource subj = ResourceFactory.createResource("http://example.org/consumer/1");
        String encoded = URLEncoder.encode(subj.getURI(), "UTF-8");
        String nt = target("/type").queryParam("uri", encoded)
                .request("application/n-triples").get(String.class);
        assertNotNull(nt);
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, new StringReader(nt), null, Lang.NT);
        assertEquals(model.size(), 1);
        List<RDFNode> actual = model.listObjectsOfProperty(subj, RDF.type).toList();
        assertEquals(actual, singletonList(toJena(Consumer)));
    }

    @Test
    public void selfTestGetPO() throws UnsupportedEncodingException {
        Resource subj = ResourceFactory.createResource("http://example.org/order/1");
        String encoded = URLEncoder.encode(subj.getURI(), "UTF-8");
        String ttl = target("/getPO").queryParam("uri", encoded)
                .request("text/turtle").get(String.class);
        assertNotNull(ttl);
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, new StringReader(ttl), null, Lang.TTL);

        Model expected = ModelFactory.createDefaultModel();
        expected.add(toJena(ex("order/1")), RDF.type, toJena(ex("Order")));
        expected.add(toJena(ex("order/1")), toJenaProperty(ex("hasConsumer")),
                     toJena(ex("consumer/1")));
        assertTrue(model.isIsomorphicWith(expected));
    }

    private void test(@Nonnull Module module, @Nonnull Op root,
                      @Nonnull Set<Solution> expected) {
        Set<Solution> all = new HashSet<>();
        PlanExecutor executor = Guice.createInjector(module).getInstance(PlanExecutor.class);
        ResultsAssert.assertExpectedResults(executor.executeNode(root), expected);
    }

    @Test(dataProvider = "modulesData")
    public void testSingleQueryNode(@Nonnull Module module) {
        EndpointQueryOp node = new EndpointQueryOp(ep, createQuery(Alice, knows, x));
        test(module, node, Sets.newHashSet(MapSolution.build(x, Bob)));
    }

    @Test(dataProvider = "modulesData")
    public void testMultiQuery(@Nonnull Module module) {
        EndpointQueryOp q1 = new EndpointQueryOp(ep, createQuery(Alice, knows, x));
        EndpointQueryOp q2 = new EndpointQueryOp(ep, createQuery(x, knows, Bob));
        Op node = UnionOp.builder().add(q1).add(q2).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(x, Bob),
                MapSolution.build(x, Alice)));
    }

    @Test(dataProvider = "modulesData")
    public void testMultiQueryDifferentVars(@Nonnull Module module) {
        EndpointQueryOp q1 = new EndpointQueryOp(ep, createQuery(Alice, knows, x));
        EndpointQueryOp q2 = new EndpointQueryOp(ep, createQuery(y, knows, Bob));
        Op node = UnionOp.builder().add(q1).add(q2).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(x, Bob),
                MapSolution.build(y, Alice)));
    }

    @Test(dataProvider = "modulesData")
    public void testCartesianProduct(@Nonnull Module module) {
        EndpointQueryOp left = new EndpointQueryOp(ep, createQuery(x, knows, Bob));
        EndpointQueryOp right = new EndpointQueryOp(ep, createQuery(Bob, name, y));

        CartesianOp node = new CartesianOp(asList(left, right));
        test(module, node, Sets.newHashSet(
                MapSolution.builder().put("x", Alice).put("y", bob).build(),
                MapSolution.builder().put("x", Alice).put("y", beto).build()
        ));

        node = new CartesianOp(asList(left, right));
        test(module, node, Sets.newHashSet(
                MapSolution.builder().put("y", bob ).put("x", Alice).build(),
                MapSolution.builder().put("y", beto).put("x", Alice).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testSingleHopPathJoin(@Nonnull Module module) {
        EndpointQueryOp l = new EndpointQueryOp(joinsEp, createQuery(ex("h1"), knows, x));
        EndpointQueryOp r = new EndpointQueryOp(joinsEp, createQuery(x, knows, ex("h3")));
        JoinOp node = JoinOp.create(l, r);
        test(module, node, Sets.newHashSet(
                MapSolution.build(x, ex("h2"))
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testTwoHopsPathJoin(@Nonnull Module module) {
        EndpointQueryOp l  = new EndpointQueryOp(joinsEp, createQuery(ex("h1"), knows, x));
        EndpointQueryOp r  = new EndpointQueryOp(joinsEp, createQuery(x, knows, y));
        EndpointQueryOp rr = new EndpointQueryOp(joinsEp, createQuery(y, knows, ex("h4")));
        JoinOp lRoot = JoinOp.create(l, r);
        JoinOp root = JoinOp.create(lRoot, rr);
        test(module, root, Sets.newHashSet(
                MapSolution.builder().put("x", ex("h2")).put("y", ex("h3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testFiveHopsPathJoinLeftDeep(@Nonnull Module module) {
        EndpointQueryOp[] leafs = {
          /*0*/ new EndpointQueryOp(joinsEp, createQuery(ex("h1"), knows, x)),
          /*1*/ new EndpointQueryOp(joinsEp, createQuery(x, knows, y)),
          /*2*/ new EndpointQueryOp(joinsEp, createQuery(y, knows, z)),
          /*3*/ new EndpointQueryOp(joinsEp, createQuery(z, knows, w)),
          /*4*/ new EndpointQueryOp(joinsEp, createQuery(w, knows, ex("h6"))),
        };
        JoinOp j0 = JoinOp.create(leafs[0], leafs[1]);
        JoinOp j1 = JoinOp.create(j0, leafs[2]);
        JoinOp j2 = JoinOp.create(j1, leafs[3]);
        JoinOp j3 = JoinOp.create(j2, leafs[4]);
        test(module, j3, singleton(MapSolution.builder().put(x, ex("h2"))
                                                        .put(y, ex("h3"))
                                                        .put(z, ex("h4"))
                                                        .put(w, ex("h5")).build()));
    }

    @Test(dataProvider = "modulesData")
    public void testFiveHopsPathJoinRightDeep(@Nonnull Module module) {
        EndpointQueryOp[] leafs = {
                /*0*/ new EndpointQueryOp(joinsEp, createQuery(ex("h1"), knows, x)),
                /*1*/ new EndpointQueryOp(joinsEp, createQuery(x, knows, y)),
                /*2*/ new EndpointQueryOp(joinsEp, createQuery(y, knows, z)),
                /*3*/ new EndpointQueryOp(joinsEp, createQuery(z, knows, w)),
                /*4*/ new EndpointQueryOp(joinsEp, createQuery(w, knows, ex("h6"))),
        };
        JoinOp j0 = JoinOp.create(leafs[4], leafs[3]);
        JoinOp j1 = JoinOp.create(j0, leafs[2]);
        JoinOp j2 = JoinOp.create(j1, leafs[1]);
        JoinOp j3 = JoinOp.create(j2, leafs[0]);
        test(module, j3, singleton(MapSolution.builder().put(x, ex("h2"))
                .put(y, ex("h3"))
                .put(z, ex("h4"))
                .put(w, ex("h5")).build()));
    }

    @Test(dataProvider = "modulesData")
    public void testFiveHopsPathJoinBalanced(@Nonnull Module module) {
        EndpointQueryOp[] leafs = {
                /*0*/ new EndpointQueryOp(joinsEp, createQuery(ex("h1"), knows, x)),
                /*1*/ new EndpointQueryOp(joinsEp, createQuery(x, knows, y)),
                /*2*/ new EndpointQueryOp(joinsEp, createQuery(y, knows, z)),
                /*3*/ new EndpointQueryOp(joinsEp, createQuery(z, knows, w)),
                /*4*/ new EndpointQueryOp(joinsEp, createQuery(w, knows, ex("h6"))),
        };
        JoinOp j0 = JoinOp.create(leafs[0], leafs[1]);
        JoinOp j1 = JoinOp.create(leafs[2], j0);
        JoinOp j2 = JoinOp.create(leafs[3], leafs[4]);
        JoinOp j3 = JoinOp.create(j1, j2);

        test(module, j3, singleton(MapSolution.builder().put(x, ex("h2"))
                                                        .put(y, ex("h3"))
                                                        .put(z, ex("h4"))
                                                        .put(w, ex("h5")).build()));
    }

    @Test(dataProvider = "modulesData")
    public void testThreeHopsMultiPathsLeftDeep(@Nonnull Module module) {
        EndpointQueryOp[] leafs = {
                /*0*/ new EndpointQueryOp(joinsEp, createQuery(ex("src"), knows, x)),
                /*1*/ new EndpointQueryOp(joinsEp, createQuery(x, knows, y)),
                /*2*/ new EndpointQueryOp(joinsEp, createQuery(y, knows, z)),
                /*3*/ new EndpointQueryOp(joinsEp, createQuery(z, knows, ex("dst"))),
        };
        JoinOp j0 = JoinOp.create(leafs[0], leafs[1]);
        JoinOp j1 = JoinOp.create(j0, leafs[2]);
        JoinOp j2 = JoinOp.create(j1, leafs[3]);

        test(module, j2, Sets.newHashSet(
                MapSolution.builder().put(x, ex("i1")).put(y, ex("i2")).put(z, ex("i3")).build(),
                MapSolution.builder().put(x, ex("j1")).put(y, ex("j2")).put(z, ex("j3")).build(),
                MapSolution.builder().put(x, ex("k1")).put(y, ex("i2")).put(z, ex("i3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testThreeHopsMultiPathsRightDeep(@Nonnull Module module) {
        EndpointQueryOp[] leafs = {
                /*0*/ new EndpointQueryOp(joinsEp, createQuery(ex("src"), knows, x)),
                /*1*/ new EndpointQueryOp(joinsEp, createQuery(x, knows, y)),
                /*2*/ new EndpointQueryOp(joinsEp, createQuery(y, knows, z)),
                /*3*/ new EndpointQueryOp(joinsEp, createQuery(z, knows, ex("dst"))),
        };
        JoinOp j0 = JoinOp.create(leafs[2], leafs[3]);
        JoinOp j1 = JoinOp.create(leafs[1], j0);
        JoinOp j2 = JoinOp.create(leafs[0], j1);

        test(module, j2, Sets.newHashSet(
                MapSolution.builder().put(x, ex("i1")).put(y, ex("i2")).put(z, ex("i3")).build(),
                MapSolution.builder().put(x, ex("j1")).put(y, ex("j2")).put(z, ex("j3")).build(),
                MapSolution.builder().put(x, ex("k1")).put(y, ex("i2")).put(z, ex("i3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testThreeHopsMultiPathsBalanced(@Nonnull Module module) {
        EndpointQueryOp[] leafs = {
                /*0*/ new EndpointQueryOp(joinsEp, createQuery(ex("src"), knows, x)),
                /*1*/ new EndpointQueryOp(joinsEp, createQuery(x, knows, y)),
                /*2*/ new EndpointQueryOp(joinsEp, createQuery(y, knows, z)),
                /*3*/ new EndpointQueryOp(joinsEp, createQuery(z, knows, ex("dst"))),
        };
        JoinOp j0 = JoinOp.create(leafs[0], leafs[1]);
        JoinOp j1 = JoinOp.create(leafs[2], leafs[3]);
        JoinOp j2 = JoinOp.create(j0, j1);

        test(module, j2, Sets.newHashSet(
                MapSolution.builder().put(x, ex("i1")).put(y, ex("i2")).put(z, ex("i3")).build(),
                MapSolution.builder().put(x, ex("j1")).put(y, ex("j2")).put(z, ex("j3")).build(),
                MapSolution.builder().put(x, ex("k1")).put(y, ex("i2")).put(z, ex("i3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testGetOrderAndConsumer(@Nonnull Module module) {
        EndpointQueryOp l = new EndpointQueryOp(joinsEp, createQuery(x, type, ex("Order")));
        EndpointQueryOp r = new EndpointQueryOp(joinsEp, createQuery(x, ex("hasConsumer"), y));
        JoinOp join = JoinOp.create(l, r);
        test(module, join, Sets.newHashSet(
                MapSolution.builder().put("x", ex("order/1")).put("y", ex("consumer/1")).build(),
                MapSolution.builder().put("x", ex("order/2")).put("y", ex("consumer/2")).build(),
                MapSolution.builder().put("x", ex("order/3")).put("y", ex("consumer/3")).build(),
                MapSolution.builder().put("x", ex("order/4")).put("y", ex("consumer/4")).build(),
                MapSolution.builder().put("x", ex("order/5")).put("y", ex("consumer/5")).build(),
                MapSolution.builder().put("x", ex("order/6")).put("y", ex("consumer/6")).build(),
                MapSolution.builder().put("x", ex("order/7")).put("y", ex("consumer/7")).build(),
                MapSolution.builder().put("x", ex("order/8")).put("y", ex("consumer/8")).build(),
                MapSolution.builder().put("x", ex("order/9")).put("y", ex("consumer/9")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testGetOrderAndPremiumConsumer(@Nonnull Module module) {
        EndpointQueryOp l = new EndpointQueryOp(joinsEp, createQuery(x, type, ex("Order")));
        EndpointQueryOp r = new EndpointQueryOp(joinsEp, createQuery(x, ex("hasConsumer"), y));
        EndpointQueryOp rr = new EndpointQueryOp(joinsEp, createQuery(y, type, ex("PremiumConsumer")));
        JoinOp rightRoot = JoinOp.create(r, rr);
        JoinOp join = JoinOp.create(l, rightRoot);
        test(module, join, Sets.newHashSet(
                MapSolution.builder().put("x", ex("order/2")).put("y", ex("consumer/2")).build(),
                MapSolution.builder().put("x", ex("order/3")).put("y", ex("consumer/3")).build(),
                MapSolution.builder().put("x", ex("order/4")).put("y", ex("consumer/4")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testGetOrderAndConsumerTypeWithProjection(@Nonnull Module module) {
        EndpointQueryOp a = new EndpointQueryOp(joinsEp, createQuery(x, type, ex("Order")));
        EndpointQueryOp b = new EndpointQueryOp(joinsEp, createQuery(x, ex("hasConsumer"), y));
        EndpointQueryOp c = new EndpointQueryOp(joinsEp, createQuery(y, type, z));
        JoinOp leftBuilder = JoinOp.create(a, b);
        JoinOp root = JoinOp.create(leftBuilder, c);
        root.modifiers().add(Projection.of("x", "z"));
        test(module, root, Sets.newHashSet(
                MapSolution.builder().put("x", ex("order/1")).put("z", ex("Consumer")).build(),
                MapSolution.builder().put("x", ex("order/2")).put("z", ex("Consumer")).build(),
                MapSolution.builder().put("x", ex("order/3")).put("z", ex("Consumer")).build(),
                MapSolution.builder().put("x", ex("order/4")).put("z", ex("Consumer")).build(),
                MapSolution.builder().put("x", ex("order/5")).put("z", ex("Consumer")).build(),
                MapSolution.builder().put("x", ex("order/6")).put("z", ex("Consumer")).build(),
                MapSolution.builder().put("x", ex("order/7")).put("z", ex("Consumer")).build(),
                MapSolution.builder().put("x", ex("order/8")).put("z", ex("Consumer")).build(),
                MapSolution.builder().put("x", ex("order/9")).put("z", ex("Consumer")).build(),
                MapSolution.builder().put("x", ex("order/2")).put("z", ex("PremiumConsumer")).build(),
                MapSolution.builder().put("x", ex("order/3")).put("z", ex("PremiumConsumer")).build(),
                MapSolution.builder().put("x", ex("order/4")).put("z", ex("PremiumConsumer")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testGetQuotesAndClients(@Nonnull Module module) {
        EndpointQueryOp l = new EndpointQueryOp(joinsEp, createQuery(x, type, ex("Quote")));
        EndpointQueryOp r = new EndpointQueryOp(joinsEp, createQuery(x, ex("hasClient"), y));
        JoinOp join = JoinOp.create(l, r);
        test(module, join, Sets.newHashSet(
                MapSolution.builder().put("x", ex("quote/1")).put("y", ex("client/1")).build(),
                MapSolution.builder().put("x", ex("quote/2")).put("y", ex("client/1")).build(),
                MapSolution.builder().put("x", ex("quote/3")).put("y", ex("client/1")).build(),
                MapSolution.builder().put("x", ex("quote/4")).put("y", ex("client/1")).build(),
                MapSolution.builder().put("x", ex("quote/5")).put("y", ex("client/1")).build(),
                MapSolution.builder().put("x", ex("quote/6")).put("y", ex("client/2")).build(),
                MapSolution.builder().put("x", ex("quote/7")).put("y", ex("client/2")).build(),
                MapSolution.builder().put("x", ex("quote/8")).put("y", ex("client/2")).build(),
                MapSolution.builder().put("x", ex("quote/9")).put("y", ex("client/2")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testProductAndCostumer(@Nonnull Module module) {
        EndpointQueryOp l = new EndpointQueryOp(joinsEp, createQuery(x, ex("soldTo"), y));
        EndpointQueryOp r = new EndpointQueryOp(joinsEp, createQuery(y, type, ex("Costumer")));
        JoinOp join = JoinOp.create(l, r);
        test(module, join, Sets.newHashSet(
                MapSolution.builder().put("x", ex("product/3")).put("y", ex("costumer/1")).build(),
                MapSolution.builder().put("x", ex("product/4")).put("y", ex("costumer/2")).build(),
                MapSolution.builder().put("x", ex("product/5")).put("y", ex("costumer/3")).build()
        ));

        join = JoinOp.create(r, l);
        test(module, join, Sets.newHashSet(
                MapSolution.builder().put("x", ex("product/3")).put("y", ex("costumer/1")).build(),
                MapSolution.builder().put("x", ex("product/4")).put("y", ex("costumer/2")).build(),
                MapSolution.builder().put("x", ex("product/5")).put("y", ex("costumer/3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testProductAndCostumerProjectingProduct(@Nonnull Module module) {
        EndpointQueryOp l = new EndpointQueryOp(joinsEp, createQuery(x, ex("soldTo"), y));
        EndpointQueryOp r = new EndpointQueryOp(joinsEp, createQuery(y, type, ex("Costumer")));
        JoinOp join = JoinOp.create(l, r);
        join.modifiers().add(Projection.of("x"));
        test(module, join, Sets.newHashSet(
                MapSolution.build(x, ex("product/3")),
                MapSolution.build(x, ex("product/4")),
                MapSolution.build(x, ex("product/5"))
        ));

        join = JoinOp.create(r, l);
        join.modifiers().add(Projection.of("x"));
        test(module, join, Sets.newHashSet(
                MapSolution.build(x, ex("product/3")),
                MapSolution.build(x, ex("product/4")),
                MapSolution.build(x, ex("product/5"))
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testProductAndCostumerProjecting(@Nonnull Module module) {
        EndpointQueryOp l = new EndpointQueryOp(joinsEp, createQuery(x, ex("soldTo"), y));
        EndpointQueryOp r = new EndpointQueryOp(joinsEp, createQuery(y, type, ex("Costumer")));
        JoinOp join = JoinOp.create(l, r);
        join.modifiers().add(Projection.of("y"));
        test(module, join, Sets.newHashSet(
                MapSolution.build(y, ex("costumer/1")),
                MapSolution.build(y, ex("costumer/2")),
                MapSolution.build(y, ex("costumer/3"))
        ));

        join = JoinOp.create(r, l);
        join.modifiers().add(Projection.of("y"));
        test(module, join, Sets.newHashSet(
                MapSolution.build(y, ex("costumer/1")),
                MapSolution.build(y, ex("costumer/2")),
                MapSolution.build(y, ex("costumer/3"))
        ));
    }

    @Test(dataProvider = "modulesData") //no joins, so any module will do
    public void testQueryApi(@Nonnull Module module) {
        StdURI consumer1 = ex("consumer/1");
        EndpointQueryOp node = new EndpointQueryOp(typeEp,
                createQuery(consumer1, AtomInputAnnotation.asRequired(typedThing, "uri").get(),
                            type, x, AtomAnnotation.of(typeAtom)));
        test(module, node, singleton(MapSolution.build(x, ex("Consumer"))));
    }

    @Test(dataProvider = "bindJoinModulesData")
    public void testBindJoinServiceAndTriple(@Nonnull Module module) {
        EndpointQueryOp q1 = new EndpointQueryOp(joinsEp, createQuery(ex("order/1"), ex("hasConsumer"), x));
        EndpointQueryOp q2 = new EndpointQueryOp(typeEp,
                createQuery(x, AtomInputAnnotation.asRequired(typedThing, "uri").get(), type, y, AtomAnnotation.of(typeAtom)));
        test(module, JoinOp.create(q1, q2), singleton(
                MapSolution.builder().put(x, ex("consumer/1")).put(y, ex("Consumer")).build()
        ));
        // order under the JoinNode should be irrelevant
        test(module, JoinOp.create(q2, q1), singleton(
                MapSolution.builder().put(x, ex("consumer/1")).put(y, ex("Consumer")).build()
        ));
    }

    @Test(dataProvider = "bindJoinModulesData")
    public void testSinglePathQueryWithThreeServices(@Nonnull Module module) {
        EndpointQueryOp n1 = new EndpointQueryOp(poEP, createQuery(
                ex("h1"), AtomInputAnnotation.asRequired(poThing, "uri").get(), knows, x, AtomAnnotation.of(knowsAtom)));
        EndpointQueryOp n2 = new EndpointQueryOp(poEP,
                createQuery(x, AtomInputAnnotation.asRequired(poThing, "uri").get(), knows, y, AtomAnnotation.of(knowsAtom)));
        EndpointQueryOp n3 = new EndpointQueryOp(poEP, createQuery(
                y, AtomInputAnnotation.asRequired(poThing, "uri").get(), knows, z, AtomAnnotation.of(knowsAtom)));
        JoinOp j1 = JoinOp.create(n1, n2);
        JoinOp j2 = JoinOp.create(j1, n3);
        test(module, j2, singleton(
                MapSolution.builder().put(x, ex("h2")).put(y, ex("h3")).put(z, ex("h4")).build()));

        // project at root
        JoinOp j3 = JoinOp.create(j1, n3);
        j3.modifiers().add(Projection.of("z"));
        test(module, j3, singleton(MapSolution.build(z, ex("h4"))));
    }

    @Test(dataProvider = "bindJoinModulesData")
    public void testMultiPathQueryWithFourServices(@Nonnull Module module) {
        EndpointQueryOp n1 = new EndpointQueryOp(poEP, createQuery(
                ex("src"), AtomInputAnnotation.asRequired(poThing, "uri").get(), knows, x, AtomAnnotation.of(knowsAtom)));
        EndpointQueryOp n2 = new EndpointQueryOp(poEP, createQuery(
                x, AtomInputAnnotation.asRequired(poThing, "uri").get(), knows, y, AtomAnnotation.of(knowsAtom)));
        EndpointQueryOp n3 = new EndpointQueryOp(poEP, createQuery(
                y, AtomInputAnnotation.asRequired(poThing, "uri").get(), knows, z, AtomAnnotation.of(knowsAtom)));
        EndpointQueryOp n4 = new EndpointQueryOp(poEP, createQuery(
                z, AtomInputAnnotation.asRequired(poThing, "uri").get(), knows, w, AtomAnnotation.of(knowsAtom)));
        JoinOp j1 = JoinOp.create(n1, n2);
        JoinOp j2 = JoinOp.create(n3, n4);
        JoinOp j3 = JoinOp.create(j1, j2);
        j3.modifiers().add(Projection.of("x", "w"));
        test(module, j3, Sets.newHashSet(
                MapSolution.builder().put(x, ex("i1")).put(w, ex("dst")).build(),
                MapSolution.builder().put(x, ex("j1")).put(w, ex("dst")).build(),
                MapSolution.builder().put(x, ex("k1")).put(w, ex("dst")).build()
        ));
    }


}