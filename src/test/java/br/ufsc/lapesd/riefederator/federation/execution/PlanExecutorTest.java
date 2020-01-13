package br.ufsc.lapesd.riefederator.federation.execution;

import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
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
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.impl.MapSolution;
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
import org.apache.jena.sparql.vocabulary.FOAF;
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
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

public class PlanExecutorTest extends JerseyTestNg.ContainerPerClassTest {
    public static final @Nonnull StdURI ALICE = new StdURI("http://example.org/Alice");
    public static final @Nonnull StdURI BOB = new StdURI("http://example.org/Bob");
    public static final @Nonnull StdURI Person = new StdURI(FOAF.Person.getURI());
    public static final @Nonnull StdURI Consumer = new StdURI("http://example.org/Consumer");
    public static final @Nonnull StdURI type = new StdURI(RDF.type.getURI());
    public static final @Nonnull StdURI knows = new StdURI(FOAF.knows.getURI());
    public static final @Nonnull StdURI name = new StdURI(FOAF.name.getURI());
    public static final @Nonnull StdLit bob = StdLit.fromUnescaped("bob", "en");
    public static final @Nonnull StdLit beto = StdLit.fromUnescaped("beto", "pt");
    public static final @Nonnull StdVar X = new StdVar("x");
    public static final @Nonnull StdVar Y = new StdVar("y");
    public static final @Nonnull StdVar Z = new StdVar("z");
    public static final @Nonnull StdVar W = new StdVar("w");

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
            try (Results r = ep.query(CQuery.from(new Triple(new StdURI(uri), type, X)))) {
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
            try (Results r = ep.query(CQuery.from(new Triple(new StdURI(uri), X, Y)))) {
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
        QueryNode node = new QueryNode(ep, CQuery.from(new Triple(ALICE, knows, X)));
        test(module, node, Sets.newHashSet(MapSolution.build(X, BOB)));
    }

    @Test(dataProvider = "modulesData")
    public void testMultiQuery(@Nonnull Module module) {
        QueryNode q1 = new QueryNode(ep, CQuery.from(new Triple(ALICE, knows, X)));
        QueryNode q2 = new QueryNode(ep, CQuery.from(new Triple(X, knows, BOB)));
        MultiQueryNode node = MultiQueryNode.builder().add(q1).add(q2).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(X, BOB),
                MapSolution.build(X, ALICE)));
    }

    @Test(dataProvider = "modulesData")
    public void testMultiQueryDifferentVars(@Nonnull Module module) {
        QueryNode q1 = new QueryNode(ep, CQuery.from(new Triple(ALICE, knows, X)));
        QueryNode q2 = new QueryNode(ep, CQuery.from(new Triple(Y, knows, BOB)));
        MultiQueryNode node = MultiQueryNode.builder().add(q1).add(q2).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(X, BOB),
                MapSolution.build(Y, ALICE)));
    }

    @Test(dataProvider = "modulesData")
    public void testMultiQueryIntersecting(@Nonnull Module module) {
        QueryNode q1 = new QueryNode(ep, CQuery.from(new Triple(ALICE, type, X)));
        QueryNode q2 = new QueryNode(ep, CQuery.from(asList(
                new Triple(ALICE, knows, X), new Triple(X, name, Y))));

        MultiQueryNode node = MultiQueryNode.builder().add(q1).add(q2).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(X, Person),
                MapSolution.builder().put("x", BOB).put("y", bob).build(),
                MapSolution.builder().put("x", BOB).put("y", beto).build()));

        // intersecting...
        node = MultiQueryNode.builder().intersect().add(q1).add(q2).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(X, Person),
                MapSolution.build(X, BOB)));
    }

    @Test(dataProvider = "modulesData")
    public void testCartesianProduct(@Nonnull Module module) {
        QueryNode left = new QueryNode(ep, CQuery.from(new Triple(X, knows, BOB)));
        QueryNode right = new QueryNode(ep, CQuery.from(new Triple(BOB, name, Y)));

        CartesianNode node = new CartesianNode(asList(left, right));
        test(module, node, Sets.newHashSet(
                MapSolution.builder().put("x", ALICE).put("y", bob).build(),
                MapSolution.builder().put("x", ALICE).put("y", beto).build()
        ));

        node = new CartesianNode(asList(left, right));
        test(module, node, Sets.newHashSet(
                MapSolution.builder().put("y", bob ).put("x", ALICE).build(),
                MapSolution.builder().put("y", beto).put("x", ALICE).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testSingleHopPathJoin(@Nonnull Module module) {
        QueryNode l = new QueryNode(joinsEp, CQuery.from(new Triple(ex("h1"), knows, X)));
        QueryNode r = new QueryNode(joinsEp, CQuery.from(new Triple(X, knows, ex("h3"))));
        JoinNode node = JoinNode.builder(l, r).build();
        test(module, node, Sets.newHashSet(
                MapSolution.build(X, ex("h2"))
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testTwoHopsPathJoin(@Nonnull Module module) {
        QueryNode l  = new QueryNode(joinsEp, CQuery.from(new Triple(ex("h1"), knows, X)));
        QueryNode r  = new QueryNode(joinsEp, CQuery.from(new Triple(X, knows, Y)));
        QueryNode rr = new QueryNode(joinsEp, CQuery.from(new Triple(Y, knows, ex("h4"))));
        JoinNode lRoot = JoinNode.builder(l, r).build();
        JoinNode  root = JoinNode.builder(lRoot, rr).build();
        test(module, root, Sets.newHashSet(
                MapSolution.builder().put("x", ex("h2")).put("y", ex("h3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testFiveHopsPathJoinLeftDeep(@Nonnull Module module) {
        QueryNode[] leafs = {
          /*0*/ new QueryNode(joinsEp, CQuery.from(new Triple(ex("h1"), knows, X))),
          /*1*/ new QueryNode(joinsEp, CQuery.from(new Triple(X       , knows, Y))),
          /*2*/ new QueryNode(joinsEp, CQuery.from(new Triple(Y       , knows, Z))),
          /*3*/ new QueryNode(joinsEp, CQuery.from(new Triple(Z       , knows, W))),
          /*4*/ new QueryNode(joinsEp, CQuery.from(new Triple(W       , knows, ex("h6")))),
        };
        JoinNode j0 = JoinNode.builder(leafs[0], leafs[1]).build();
        JoinNode j1 = JoinNode.builder(j0, leafs[2]).build();
        JoinNode j2 = JoinNode.builder(j1, leafs[3]).build();
        JoinNode j3 = JoinNode.builder(j2, leafs[4]).build();
        test(module, j3, singleton(MapSolution.builder().put(X, ex("h2"))
                                                        .put(Y, ex("h3"))
                                                        .put(Z, ex("h4"))
                                                        .put(W, ex("h5")).build()));
    }

    @Test(dataProvider = "modulesData")
    public void testFiveHopsPathJoinRightDeep(@Nonnull Module module) {
        QueryNode[] leafs = {
                /*0*/ new QueryNode(joinsEp, CQuery.from(new Triple(ex("h1"), knows, X))),
                /*1*/ new QueryNode(joinsEp, CQuery.from(new Triple(X       , knows, Y))),
                /*2*/ new QueryNode(joinsEp, CQuery.from(new Triple(Y       , knows, Z))),
                /*3*/ new QueryNode(joinsEp, CQuery.from(new Triple(Z       , knows, W))),
                /*4*/ new QueryNode(joinsEp, CQuery.from(new Triple(W       , knows, ex("h6")))),
        };
        JoinNode j0 = JoinNode.builder(leafs[4], leafs[3]).build();
        JoinNode j1 = JoinNode.builder(j0, leafs[2]).build();
        JoinNode j2 = JoinNode.builder(j1, leafs[1]).build();
        JoinNode j3 = JoinNode.builder(j2, leafs[0]).build();
        test(module, j3, singleton(MapSolution.builder().put(X, ex("h2"))
                .put(Y, ex("h3"))
                .put(Z, ex("h4"))
                .put(W, ex("h5")).build()));
    }

    @Test(dataProvider = "modulesData")
    public void testFiveHopsPathJoinBalanced(@Nonnull Module module) {
        QueryNode[] leafs = {
                /*0*/ new QueryNode(joinsEp, CQuery.from(new Triple(ex("h1"), knows, X))),
                /*1*/ new QueryNode(joinsEp, CQuery.from(new Triple(X       , knows, Y))),
                /*2*/ new QueryNode(joinsEp, CQuery.from(new Triple(Y       , knows, Z))),
                /*3*/ new QueryNode(joinsEp, CQuery.from(new Triple(Z       , knows, W))),
                /*4*/ new QueryNode(joinsEp, CQuery.from(new Triple(W       , knows, ex("h6")))),
        };
        JoinNode j0 = JoinNode.builder(leafs[0], leafs[1]).build();
        JoinNode j1 = JoinNode.builder(leafs[2], j0).build();
        JoinNode j2 = JoinNode.builder(leafs[3], leafs[4]).build();
        JoinNode j3 = JoinNode.builder(j1, j2).build();

        test(module, j3, singleton(MapSolution.builder().put(X, ex("h2"))
                                                        .put(Y, ex("h3"))
                                                        .put(Z, ex("h4"))
                                                        .put(W, ex("h5")).build()));
    }

    @Test(dataProvider = "modulesData")
    public void testThreeHopsMultiPathsLeftDeep(@Nonnull Module module) {
        QueryNode[] leafs = {
                /*0*/ new QueryNode(joinsEp, CQuery.from(new Triple(ex("src"), knows, X        ))),
                /*1*/ new QueryNode(joinsEp, CQuery.from(new Triple(X        , knows, Y        ))),
                /*2*/ new QueryNode(joinsEp, CQuery.from(new Triple(Y        , knows, Z        ))),
                /*3*/ new QueryNode(joinsEp, CQuery.from(new Triple(Z        , knows, ex("dst")))),
        };
        JoinNode j0 = JoinNode.builder(leafs[0], leafs[1]).build();
        JoinNode j1 = JoinNode.builder(j0, leafs[2]).build();
        JoinNode j2 = JoinNode.builder(j1, leafs[3]).build();

        test(module, j2, Sets.newHashSet(
                MapSolution.builder().put(X, ex("i1")).put(Y, ex("i2")).put(Z, ex("i3")).build(),
                MapSolution.builder().put(X, ex("j1")).put(Y, ex("j2")).put(Z, ex("j3")).build(),
                MapSolution.builder().put(X, ex("k1")).put(Y, ex("i2")).put(Z, ex("i3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testThreeHopsMultiPathsRightDeep(@Nonnull Module module) {
        QueryNode[] leafs = {
                /*0*/ new QueryNode(joinsEp, CQuery.from(new Triple(ex("src"), knows, X        ))),
                /*1*/ new QueryNode(joinsEp, CQuery.from(new Triple(X        , knows, Y        ))),
                /*2*/ new QueryNode(joinsEp, CQuery.from(new Triple(Y        , knows, Z        ))),
                /*3*/ new QueryNode(joinsEp, CQuery.from(new Triple(Z        , knows, ex("dst")))),
        };
        JoinNode j0 = JoinNode.builder(leafs[2], leafs[3]).build();
        JoinNode j1 = JoinNode.builder(leafs[1], j0).build();
        JoinNode j2 = JoinNode.builder(leafs[0], j1).build();

        test(module, j2, Sets.newHashSet(
                MapSolution.builder().put(X, ex("i1")).put(Y, ex("i2")).put(Z, ex("i3")).build(),
                MapSolution.builder().put(X, ex("j1")).put(Y, ex("j2")).put(Z, ex("j3")).build(),
                MapSolution.builder().put(X, ex("k1")).put(Y, ex("i2")).put(Z, ex("i3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testThreeHopsMultiPathsBalanced(@Nonnull Module module) {
        QueryNode[] leafs = {
                /*0*/ new QueryNode(joinsEp, CQuery.from(new Triple(ex("src"), knows, X        ))),
                /*1*/ new QueryNode(joinsEp, CQuery.from(new Triple(X        , knows, Y        ))),
                /*2*/ new QueryNode(joinsEp, CQuery.from(new Triple(Y        , knows, Z        ))),
                /*3*/ new QueryNode(joinsEp, CQuery.from(new Triple(Z        , knows, ex("dst")))),
        };
        JoinNode j0 = JoinNode.builder(leafs[0], leafs[1]).build();
        JoinNode j1 = JoinNode.builder(leafs[2], leafs[3]).build();
        JoinNode j2 = JoinNode.builder(j0, j1).build();

        test(module, j2, Sets.newHashSet(
                MapSolution.builder().put(X, ex("i1")).put(Y, ex("i2")).put(Z, ex("i3")).build(),
                MapSolution.builder().put(X, ex("j1")).put(Y, ex("j2")).put(Z, ex("j3")).build(),
                MapSolution.builder().put(X, ex("k1")).put(Y, ex("i2")).put(Z, ex("i3")).build()
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testGetOrderAndConsumer(@Nonnull Module module) {
        QueryNode l = new QueryNode(joinsEp, CQuery.from(new Triple(X, type, ex("Order"))));
        QueryNode r = new QueryNode(joinsEp, CQuery.from(new Triple(X, ex("hasConsumer"), Y)));
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
        QueryNode l = new QueryNode(joinsEp, CQuery.from(new Triple(X, type, ex("Order"))));
        QueryNode r = new QueryNode(joinsEp, CQuery.from(new Triple(X, ex("hasConsumer"), Y)));
        QueryNode rr = new QueryNode(joinsEp, CQuery.from(new Triple(Y, type, ex("PremiumConsumer"))));
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
        QueryNode a = new QueryNode(joinsEp, CQuery.from(new Triple(X, type, ex("Order"))));
        QueryNode b = new QueryNode(joinsEp, CQuery.from(new Triple(X, ex("hasConsumer"), Y)));
        QueryNode c = new QueryNode(joinsEp, CQuery.from(new Triple(Y, type, Z)));
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
        QueryNode l = new QueryNode(joinsEp, CQuery.from(new Triple(X, type, ex("Quote"))));
        QueryNode r = new QueryNode(joinsEp, CQuery.from(new Triple(X, ex("hasClient"), Y)));
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
        QueryNode l = new QueryNode(joinsEp, CQuery.from(new Triple(X, ex("soldTo"), Y)));
        QueryNode r = new QueryNode(joinsEp, CQuery.from(new Triple(Y, type, ex("Costumer"))));
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
        QueryNode l = new QueryNode(joinsEp, CQuery.from(new Triple(X, ex("soldTo"), Y)));
        QueryNode r = new QueryNode(joinsEp, CQuery.from(new Triple(Y, type, ex("Costumer"))));
        JoinNode join = JoinNode.builder(l, r).addResultVar("x").build();
        test(module, join, Sets.newHashSet(
                MapSolution.build(X, ex("product/3")),
                MapSolution.build(X, ex("product/4")),
                MapSolution.build(X, ex("product/5"))
        ));

        join = JoinNode.builder(r, l).addResultVar("x").build();
        test(module, join, Sets.newHashSet(
                MapSolution.build(X, ex("product/3")),
                MapSolution.build(X, ex("product/4")),
                MapSolution.build(X, ex("product/5"))
        ));
    }

    @Test(dataProvider = "modulesData")
    public void testProductAndCostumerProjecting(@Nonnull Module module) {
        QueryNode l = new QueryNode(joinsEp, CQuery.from(new Triple(X, ex("soldTo"), Y)));
        QueryNode r = new QueryNode(joinsEp, CQuery.from(new Triple(Y, type, ex("Costumer"))));
        JoinNode join = JoinNode.builder(l, r).addResultVar("y").build();
        test(module, join, Sets.newHashSet(
                MapSolution.build(Y, ex("costumer/1")),
                MapSolution.build(Y, ex("costumer/2")),
                MapSolution.build(Y, ex("costumer/3"))
        ));

        join = JoinNode.builder(r, l).addResultVar("y").build();
        test(module, join, Sets.newHashSet(
                MapSolution.build(Y, ex("costumer/1")),
                MapSolution.build(Y, ex("costumer/2")),
                MapSolution.build(Y, ex("costumer/3"))
        ));
    }

    @Test(dataProvider = "modulesData") //no joins, so any module will do
    public void testQueryApi(@Nonnull Module module) {
        StdURI consumer1 = ex("consumer/1");
        QueryNode node = new QueryNode(typeEp, CQuery.with(new Triple(consumer1, type, X))
                .annotate(consumer1, AtomAnnotation.asRequired(typedThing))
                .annotate(X, AtomAnnotation.of(typeAtom)).build());
        test(module, node, singleton(MapSolution.build(X, ex("Consumer"))));
    }

    @Test(dataProvider = "bindJoinModulesData")
    public void testBindJoinServiceAndTriple(@Nonnull Module module) {
        QueryNode q1 = new QueryNode(joinsEp, CQuery.from(
                new Triple(ex("order/1"), ex("hasConsumer"), X)));
        QueryNode q2 = new QueryNode(typeEp, CQuery.with(new Triple(X, type, Y))
                .annotate(X, AtomAnnotation.asRequired(typedThing))
                .annotate(Y, AtomAnnotation.of(typeAtom)).build());
        test(module, JoinNode.builder(q1, q2).build(), singleton(
                MapSolution.builder().put(X, ex("consumer/1")).put(Y, ex("Consumer")).build()
        ));
        // order under the JoinNode should be irrelevant
        test(module, JoinNode.builder(q2, q1).build(), singleton(
                MapSolution.builder().put(X, ex("consumer/1")).put(Y, ex("Consumer")).build()
        ));
    }

    @Test(dataProvider = "bindJoinModulesData")
    public void testSinglePathQueryWithThreeServices(@Nonnull Module module) {
        QueryNode n1 = new QueryNode(poEP, CQuery.with(new Triple(ex("h1"), knows, X))
                .annotate(ex("h1"), AtomAnnotation.asRequired(poThing))
                .annotate(X, AtomAnnotation.of(knowsAtom)).build());
        QueryNode n2 = new QueryNode(poEP, CQuery.with(new Triple(X, knows, Y))
                .annotate(X, AtomAnnotation.asRequired(poThing))
                .annotate(Y, AtomAnnotation.of(knowsAtom)).build());
        QueryNode n3 = new QueryNode(poEP, CQuery.with(new Triple(Y, knows, Z))
                .annotate(Y, AtomAnnotation.asRequired(poThing))
                .annotate(Z, AtomAnnotation.of(knowsAtom)).build());
        JoinNode j1 = JoinNode.builder(n1, n2).build();
        JoinNode j2 = JoinNode.builder(j1, n3).build();
        test(module, j2, singleton(
                MapSolution.builder().put(X, ex("h2")).put(Y, ex("h3")).put(Z, ex("h4")).build()));

        // project at root
        JoinNode j3 = JoinNode.builder(j1, n3).addResultVar("z").build();
        test(module, j3, singleton(MapSolution.build(Z, ex("h4"))));
    }

    @Test(dataProvider = "bindJoinModulesData")
    public void testMultiPathQueryWithFourServices(@Nonnull Module module) {
        QueryNode n1 = new QueryNode(poEP, CQuery.with(new Triple(ex("src"), knows, X))
                .annotate(ex("src"), AtomAnnotation.asRequired(poThing))
                .annotate(X, AtomAnnotation.of(knowsAtom)).build());
        QueryNode n2 = new QueryNode(poEP, CQuery.with(new Triple(X, knows, Y))
                .annotate(X, AtomAnnotation.asRequired(poThing))
                .annotate(Y, AtomAnnotation.of(knowsAtom)).build());
        QueryNode n3 = new QueryNode(poEP, CQuery.with(new Triple(Y, knows, Z))
                .annotate(Y, AtomAnnotation.asRequired(poThing))
                .annotate(Z, AtomAnnotation.of(knowsAtom)).build());
        QueryNode n4 = new QueryNode(poEP, CQuery.with(new Triple(Z, knows, W))
                .annotate(Z, AtomAnnotation.asRequired(poThing))
                .annotate(W, AtomAnnotation.of(knowsAtom)).build());
        JoinNode j1 = JoinNode.builder(n1, n2).build();
        JoinNode j2 = JoinNode.builder(n3, n4).build();
        JoinNode j3 = JoinNode.builder(j1, j2).addResultVars(asList("x", "w")).build();
        test(module, j3, Sets.newHashSet(
                MapSolution.builder().put(X, ex("i1")).put(W, ex("dst")).build(),
                MapSolution.builder().put(X, ex("j1")).put(W, ex("dst")).build(),
                MapSolution.builder().put(X, ex("k1")).put(W, ex("dst")).build()
        ));
    }


}