package br.ufsc.lapesd.riefederator.federation.execution;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.federation.execution.tree.*;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleCartesianNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleEmptyNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.SimpleQueryNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.FixedBindJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.FixedHashJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.HashJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.SimpleJoinNodeExecutor;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.BindJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.bind.SimpleBindJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.HashJoinResultsFactory;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.InMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.execution.tree.impl.joins.hash.ParallelInMemoryHashJoinResults;
import br.ufsc.lapesd.riefederator.federation.tree.*;
import br.ufsc.lapesd.riefederator.jena.model.term.JenaRes;
import br.ufsc.lapesd.riefederator.jena.query.ARQEndpoint;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.MapSolution;
import br.ufsc.lapesd.riefederator.webapis.WebAPICQEndpoint;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation;
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
import static br.ufsc.lapesd.riefederator.webapis.description.AtomAnnotation.asRequired;
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
            bind(QueryNodeExecutor.class).to(SimpleQueryNodeExecutor.class);
            bind(MultiQueryNodeExecutor.class).to(SimpleQueryNodeExecutor.class);
            bind(CartesianNodeExecutor.class).to(SimpleCartesianNodeExecutor.class);
            bind(EmptyNodeExecutor.class).toInstance(SimpleEmptyNodeExecutor.INSTANCE);
            bind(PlanExecutor.class).to(InjectedExecutor.class);
        }
    }

    public static final @Nonnull List<SimpleModule> modules = asList(
            new SimpleModule() {
                @Override
                protected void configure() { //Simple* implementations
                    super.configure();
                    bind(JoinNodeExecutor.class).to(FixedHashJoinNodeExecutor.class);
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
                    bind(JoinNodeExecutor.class).to(FixedHashJoinNodeExecutor.class);
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
                    bind(JoinNodeExecutor.class).to(HashJoinNodeExecutor.class);
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
                    bind(JoinNodeExecutor.class).to(SimpleJoinNodeExecutor.class);
                }
                @Override
                public @Nonnull String toString() {
                    return "SimpleJoinNodeExecutor";
                }
            },
            new SimpleModule(true) {
                @Override
                protected void configure() {
                    super.configure();
                    bind(JoinNodeExecutor.class).to(FixedBindJoinNodeExecutor.class);
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

    private void test(@Nonnull Module module, @Nonnull PlanNode root,
                      @Nonnull Set<Solution> expected) {
        Set<Solution> all = new HashSet<>();
        PlanExecutor executor = Guice.createInjector(module).getInstance(PlanExecutor.class);
        executor.executeNode(root).forEachRemainingThenClose(all::add);
        assertEquals(all, expected);

        all.clear();
        executor.executePlan(root).forEachRemainingThenClose(all::add);
        assertEquals(all, expected);
    }

    @Test(dataProvider = "modulesData")
    public void testSingleQueryNode(@Nonnull Module module) {
        QueryNode node = new QueryNode(ep, createQuery(Alice, knows, x));
        test(module, node, Sets.newHashSet(MapSolution.build(x, Bob)));
    }

    @Test(dataProvider = "modulesData")
    public void testMultiQuery(@Nonnull Module module) {
        QueryNode q1 = new QueryNode(ep, createQuery(Alice, knows, x));
        QueryNode q2 = new QueryNode(ep, createQuery(x, knows, Bob));
        MultiQueryNode node = MultiQueryNode.builder().add(q1).add(q2).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(x, Bob),
                MapSolution.build(x, Alice)));
    }

    @Test(dataProvider = "modulesData")
    public void testMultiQueryDifferentVars(@Nonnull Module module) {
        QueryNode q1 = new QueryNode(ep, createQuery(Alice, knows, x));
        QueryNode q2 = new QueryNode(ep, createQuery(y, knows, Bob));
        MultiQueryNode node = MultiQueryNode.builder().add(q1).add(q2).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(x, Bob),
                MapSolution.build(y, Alice)));
    }

    @Test(dataProvider = "modulesData")
    public void testMultiQueryIntersecting(@Nonnull Module module) {
        QueryNode q1 = new QueryNode(ep, createQuery(Alice, type, x));
        QueryNode q2 = new QueryNode(ep, createQuery(Alice, knows, x, x, name, y));

        MultiQueryNode node = MultiQueryNode.builder().add(q1).add(q2).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(x, Person),
                MapSolution.builder().put("x", Bob).put("y", bob).build(),
                MapSolution.builder().put("x", Bob).put("y", beto).build()));

        // intersecting...
        node = MultiQueryNode.builder().intersect().add(q1).add(q2).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(x, Person),
                MapSolution.build(x, Bob)));
    }

    @Test(dataProvider = "modulesData")
    public void testCartesianProduct(@Nonnull Module module) {
        QueryNode left = new QueryNode(ep, createQuery(x, knows, Bob));
        QueryNode right = new QueryNode(ep, createQuery(Bob, name, y));

        CartesianNode node = new CartesianNode(asList(left, right));
        test(module, node, Sets.newHashSet(
                MapSolution.builder().put("x", Alice).put("y", bob).build(),
                MapSolution.builder().put("x", Alice).put("y", beto).build()
        ));

        node = new CartesianNode(asList(left, right));
        test(module, node, Sets.newHashSet(
                MapSolution.builder().put("y", bob ).put("x", Alice).build(),
                MapSolution.builder().put("y", beto).put("x", Alice).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testSingleHopPathJoin(@Nonnull Module module) {
        QueryNode l = new QueryNode(joinsEp, createQuery(ex("h1"), knows, x));
        QueryNode r = new QueryNode(joinsEp, createQuery(x, knows, ex("h3")));
        JoinNode node = JoinNode.builder(l, r).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(x, ex("h2"))
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testTwoHopsPathJoin(@Nonnull Module module) {
        QueryNode l  = new QueryNode(joinsEp, createQuery(ex("h1"), knows, x));
        QueryNode r  = new QueryNode(joinsEp, createQuery(x, knows, y));
        QueryNode rr = new QueryNode(joinsEp, createQuery(y, knows, ex("h4")));
        JoinNode lRoot = JoinNode.builder(l, r).build();
        JoinNode  root = JoinNode.builder(lRoot, rr).build();
        test(module, root, Sets.newHashSet(
                MapSolution.builder().put("x", ex("h2")).put("y", ex("h3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testFiveHopsPathJoinLeftDeep(@Nonnull Module module) {
        QueryNode[] leafs = {
          /*0*/ new QueryNode(joinsEp, createQuery(ex("h1"), knows, x)),
          /*1*/ new QueryNode(joinsEp, createQuery(x, knows, y)),
          /*2*/ new QueryNode(joinsEp, createQuery(y, knows, z)),
          /*3*/ new QueryNode(joinsEp, createQuery(z, knows, w)),
          /*4*/ new QueryNode(joinsEp, createQuery(w, knows, ex("h6"))),
        };
        JoinNode j0 = JoinNode.builder(leafs[0], leafs[1]).build();
        JoinNode j1 = JoinNode.builder(j0, leafs[2]).build();
        JoinNode j2 = JoinNode.builder(j1, leafs[3]).build();
        JoinNode j3 = JoinNode.builder(j2, leafs[4]).build();
        test(module, j3, singleton(MapSolution.builder().put(x, ex("h2"))
                                                        .put(y, ex("h3"))
                                                        .put(z, ex("h4"))
                                                        .put(w, ex("h5")).build()));
    }

    @Test(dataProvider = "modulesData")
    public void testFiveHopsPathJoinRightDeep(@Nonnull Module module) {
        QueryNode[] leafs = {
                /*0*/ new QueryNode(joinsEp, createQuery(ex("h1"), knows, x)),
                /*1*/ new QueryNode(joinsEp, createQuery(x, knows, y)),
                /*2*/ new QueryNode(joinsEp, createQuery(y, knows, z)),
                /*3*/ new QueryNode(joinsEp, createQuery(z, knows, w)),
                /*4*/ new QueryNode(joinsEp, createQuery(w, knows, ex("h6"))),
        };
        JoinNode j0 = JoinNode.builder(leafs[4], leafs[3]).build();
        JoinNode j1 = JoinNode.builder(j0, leafs[2]).build();
        JoinNode j2 = JoinNode.builder(j1, leafs[1]).build();
        JoinNode j3 = JoinNode.builder(j2, leafs[0]).build();
        test(module, j3, singleton(MapSolution.builder().put(x, ex("h2"))
                .put(y, ex("h3"))
                .put(z, ex("h4"))
                .put(w, ex("h5")).build()));
    }

    @Test(dataProvider = "modulesData")
    public void testFiveHopsPathJoinBalanced(@Nonnull Module module) {
        QueryNode[] leafs = {
                /*0*/ new QueryNode(joinsEp, createQuery(ex("h1"), knows, x)),
                /*1*/ new QueryNode(joinsEp, createQuery(x, knows, y)),
                /*2*/ new QueryNode(joinsEp, createQuery(y, knows, z)),
                /*3*/ new QueryNode(joinsEp, createQuery(z, knows, w)),
                /*4*/ new QueryNode(joinsEp, createQuery(w, knows, ex("h6"))),
        };
        JoinNode j0 = JoinNode.builder(leafs[0], leafs[1]).build();
        JoinNode j1 = JoinNode.builder(leafs[2], j0).build();
        JoinNode j2 = JoinNode.builder(leafs[3], leafs[4]).build();
        JoinNode j3 = JoinNode.builder(j1, j2).build();

        test(module, j3, singleton(MapSolution.builder().put(x, ex("h2"))
                                                        .put(y, ex("h3"))
                                                        .put(z, ex("h4"))
                                                        .put(w, ex("h5")).build()));
    }

    @Test(dataProvider = "modulesData")
    public void testThreeHopsMultiPathsLeftDeep(@Nonnull Module module) {
        QueryNode[] leafs = {
                /*0*/ new QueryNode(joinsEp, createQuery(ex("src"), knows, x)),
                /*1*/ new QueryNode(joinsEp, createQuery(x, knows, y)),
                /*2*/ new QueryNode(joinsEp, createQuery(y, knows, z)),
                /*3*/ new QueryNode(joinsEp, createQuery(z, knows, ex("dst"))),
        };
        JoinNode j0 = JoinNode.builder(leafs[0], leafs[1]).build();
        JoinNode j1 = JoinNode.builder(j0, leafs[2]).build();
        JoinNode j2 = JoinNode.builder(j1, leafs[3]).build();

        test(module, j2, Sets.newHashSet(
                MapSolution.builder().put(x, ex("i1")).put(y, ex("i2")).put(z, ex("i3")).build(),
                MapSolution.builder().put(x, ex("j1")).put(y, ex("j2")).put(z, ex("j3")).build(),
                MapSolution.builder().put(x, ex("k1")).put(y, ex("i2")).put(z, ex("i3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testThreeHopsMultiPathsRightDeep(@Nonnull Module module) {
        QueryNode[] leafs = {
                /*0*/ new QueryNode(joinsEp, createQuery(ex("src"), knows, x)),
                /*1*/ new QueryNode(joinsEp, createQuery(x, knows, y)),
                /*2*/ new QueryNode(joinsEp, createQuery(y, knows, z)),
                /*3*/ new QueryNode(joinsEp, createQuery(z, knows, ex("dst"))),
        };
        JoinNode j0 = JoinNode.builder(leafs[2], leafs[3]).build();
        JoinNode j1 = JoinNode.builder(leafs[1], j0).build();
        JoinNode j2 = JoinNode.builder(leafs[0], j1).build();

        test(module, j2, Sets.newHashSet(
                MapSolution.builder().put(x, ex("i1")).put(y, ex("i2")).put(z, ex("i3")).build(),
                MapSolution.builder().put(x, ex("j1")).put(y, ex("j2")).put(z, ex("j3")).build(),
                MapSolution.builder().put(x, ex("k1")).put(y, ex("i2")).put(z, ex("i3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testThreeHopsMultiPathsBalanced(@Nonnull Module module) {
        QueryNode[] leafs = {
                /*0*/ new QueryNode(joinsEp, createQuery(ex("src"), knows, x)),
                /*1*/ new QueryNode(joinsEp, createQuery(x, knows, y)),
                /*2*/ new QueryNode(joinsEp, createQuery(y, knows, z)),
                /*3*/ new QueryNode(joinsEp, createQuery(z, knows, ex("dst"))),
        };
        JoinNode j0 = JoinNode.builder(leafs[0], leafs[1]).build();
        JoinNode j1 = JoinNode.builder(leafs[2], leafs[3]).build();
        JoinNode j2 = JoinNode.builder(j0, j1).build();

        test(module, j2, Sets.newHashSet(
                MapSolution.builder().put(x, ex("i1")).put(y, ex("i2")).put(z, ex("i3")).build(),
                MapSolution.builder().put(x, ex("j1")).put(y, ex("j2")).put(z, ex("j3")).build(),
                MapSolution.builder().put(x, ex("k1")).put(y, ex("i2")).put(z, ex("i3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testGetOrderAndConsumer(@Nonnull Module module) {
        QueryNode l = new QueryNode(joinsEp, createQuery(x, type, ex("Order")));
        QueryNode r = new QueryNode(joinsEp, createQuery(x, ex("hasConsumer"), y));
        JoinNode join = JoinNode.builder(l, r).build();
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
        QueryNode l = new QueryNode(joinsEp, createQuery(x, type, ex("Order")));
        QueryNode r = new QueryNode(joinsEp, createQuery(x, ex("hasConsumer"), y));
        QueryNode rr = new QueryNode(joinsEp, createQuery(y, type, ex("PremiumConsumer")));
        JoinNode rightRoot = JoinNode.builder(r, rr).build();
        JoinNode join = JoinNode.builder(l, rightRoot).build();
        test(module, join, Sets.newHashSet(
                MapSolution.builder().put("x", ex("order/2")).put("y", ex("consumer/2")).build(),
                MapSolution.builder().put("x", ex("order/3")).put("y", ex("consumer/3")).build(),
                MapSolution.builder().put("x", ex("order/4")).put("y", ex("consumer/4")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testGetOrderAndConsumerTypeWithProjection(@Nonnull Module module) {
        QueryNode a = new QueryNode(joinsEp, createQuery(x, type, ex("Order")));
        QueryNode b = new QueryNode(joinsEp, createQuery(x, ex("hasConsumer"), y));
        QueryNode c = new QueryNode(joinsEp, createQuery(y, type, z));
        JoinNode leftBuilder = JoinNode.builder(a, b).build();
        JoinNode root = JoinNode.builder(leftBuilder, c).addResultVars(asList("x", "z")).build();
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
        QueryNode l = new QueryNode(joinsEp, createQuery(x, type, ex("Quote")));
        QueryNode r = new QueryNode(joinsEp, createQuery(x, ex("hasClient"), y));
        JoinNode join = JoinNode.builder(l, r).build();
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
        QueryNode l = new QueryNode(joinsEp, createQuery(x, ex("soldTo"), y));
        QueryNode r = new QueryNode(joinsEp, createQuery(y, type, ex("Costumer")));
        JoinNode join = JoinNode.builder(l, r).build();
        test(module, join, Sets.newHashSet(
                MapSolution.builder().put("x", ex("product/3")).put("y", ex("costumer/1")).build(),
                MapSolution.builder().put("x", ex("product/4")).put("y", ex("costumer/2")).build(),
                MapSolution.builder().put("x", ex("product/5")).put("y", ex("costumer/3")).build()
        ));

        join = JoinNode.builder(r, l).build();
        test(module, join, Sets.newHashSet(
                MapSolution.builder().put("x", ex("product/3")).put("y", ex("costumer/1")).build(),
                MapSolution.builder().put("x", ex("product/4")).put("y", ex("costumer/2")).build(),
                MapSolution.builder().put("x", ex("product/5")).put("y", ex("costumer/3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testProductAndCostumerProjectingProduct(@Nonnull Module module) {
        QueryNode l = new QueryNode(joinsEp, createQuery(x, ex("soldTo"), y));
        QueryNode r = new QueryNode(joinsEp, createQuery(y, type, ex("Costumer")));
        JoinNode join = JoinNode.builder(l, r).addResultVar("x").build();
        test(module, join, Sets.newHashSet(
                MapSolution.build(x, ex("product/3")),
                MapSolution.build(x, ex("product/4")),
                MapSolution.build(x, ex("product/5"))
        ));

        join = JoinNode.builder(r, l).addResultVar("x").build();
        test(module, join, Sets.newHashSet(
                MapSolution.build(x, ex("product/3")),
                MapSolution.build(x, ex("product/4")),
                MapSolution.build(x, ex("product/5"))
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testProductAndCostumerProjecting(@Nonnull Module module) {
        QueryNode l = new QueryNode(joinsEp, createQuery(x, ex("soldTo"), y));
        QueryNode r = new QueryNode(joinsEp, createQuery(y, type, ex("Costumer")));
        JoinNode join = JoinNode.builder(l, r).addResultVar("y").build();
        test(module, join, Sets.newHashSet(
                MapSolution.build(y, ex("costumer/1")),
                MapSolution.build(y, ex("costumer/2")),
                MapSolution.build(y, ex("costumer/3"))
        ));

        join = JoinNode.builder(r, l).addResultVar("y").build();
        test(module, join, Sets.newHashSet(
                MapSolution.build(y, ex("costumer/1")),
                MapSolution.build(y, ex("costumer/2")),
                MapSolution.build(y, ex("costumer/3"))
        ));
    }

    @Test(dataProvider = "modulesData") //no joins, so any module will do
    public void testQueryApi(@Nonnull Module module) {
        StdURI consumer1 = ex("consumer/1");
        QueryNode node = new QueryNode(typeEp,
                createQuery(consumer1, asRequired(typedThing, "uri"),
                            type, x, AtomAnnotation.of(typeAtom)));
        test(module, node, singleton(MapSolution.build(x, ex("Consumer"))));
    }

    @Test(dataProvider = "bindJoinModulesData")
    public void testBindJoinServiceAndTriple(@Nonnull Module module) {
        QueryNode q1 = new QueryNode(joinsEp, createQuery(ex("order/1"), ex("hasConsumer"), x));
        QueryNode q2 = new QueryNode(typeEp,
                createQuery(x, asRequired(typedThing, "uri"), type, y, AtomAnnotation.of(typeAtom)));
        test(module, JoinNode.builder(q1, q2).build(), singleton(
                MapSolution.builder().put(x, ex("consumer/1")).put(y, ex("Consumer")).build()
        ));
        // order under the JoinNode should be irrelevant
        test(module, JoinNode.builder(q2, q1).build(), singleton(
                MapSolution.builder().put(x, ex("consumer/1")).put(y, ex("Consumer")).build()
        ));
    }

    @Test(dataProvider = "bindJoinModulesData")
    public void testSinglePathQueryWithThreeServices(@Nonnull Module module) {
        QueryNode n1 = new QueryNode(poEP, createQuery(
                ex("h1"), asRequired(poThing, "uri"), knows, x, AtomAnnotation.of(knowsAtom)));
        QueryNode n2 = new QueryNode(poEP,
                createQuery(x, asRequired(poThing, "uri"), knows, y, AtomAnnotation.of(knowsAtom)));
        QueryNode n3 = new QueryNode(poEP, createQuery(
                y, asRequired(poThing, "uri"), knows, z, AtomAnnotation.of(knowsAtom)));
        JoinNode j1 = JoinNode.builder(n1, n2).build();
        JoinNode j2 = JoinNode.builder(j1, n3).build();
        test(module, j2, singleton(
                MapSolution.builder().put(x, ex("h2")).put(y, ex("h3")).put(z, ex("h4")).build()));

        // project at root
        JoinNode j3 = JoinNode.builder(j1, n3).addResultVar("z").build();
        test(module, j3, singleton(MapSolution.build(z, ex("h4"))));
    }

    @Test(dataProvider = "bindJoinModulesData")
    public void testMultiPathQueryWithFourServices(@Nonnull Module module) {
        QueryNode n1 = new QueryNode(poEP, createQuery(
                ex("src"), asRequired(poThing, "uri"), knows, x, AtomAnnotation.of(knowsAtom)));
        QueryNode n2 = new QueryNode(poEP, createQuery(
                x, asRequired(poThing, "uri"), knows, y, AtomAnnotation.of(knowsAtom)));
        QueryNode n3 = new QueryNode(poEP, createQuery(
                y, asRequired(poThing, "uri"), knows, z, AtomAnnotation.of(knowsAtom)));
        QueryNode n4 = new QueryNode(poEP, createQuery(
                z, asRequired(poThing, "uri"), knows, w, AtomAnnotation.of(knowsAtom)));
        JoinNode j1 = JoinNode.builder(n1, n2).build();
        JoinNode j2 = JoinNode.builder(n3, n4).build();
        JoinNode j3 = JoinNode.builder(j1, j2).addResultVars(asList("x", "w")).build();
        test(module, j3, Sets.newHashSet(
                MapSolution.builder().put(x, ex("i1")).put(w, ex("dst")).build(),
                MapSolution.builder().put(x, ex("j1")).put(w, ex("dst")).build(),
                MapSolution.builder().put(x, ex("k1")).put(w, ex("dst")).build()
        ));
    }


}