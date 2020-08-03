package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import br.ufsc.lapesd.riefederator.webapis.description.APIMoleculeMatcher;
import br.ufsc.lapesd.riefederator.webapis.description.AtomInputAnnotation;
import br.ufsc.lapesd.riefederator.webapis.description.PureDescriptive;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.ModelMessageBodyWriter;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.UriTemplateExecutor;
import br.ufsc.lapesd.riefederator.webapis.requests.paging.impl.ParamPagingStrategy;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.uri.UriTemplate;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;
import static org.testng.Assert.*;

public class WebAPICQEndpointTest
        extends JerseyTestNg.ContainerPerClassTest implements TestContext {
    private static final URI op1 = new StdURI("http://example.org/op1");
    private static final URI exponent = new StdURI("http://example.org/exponent");
    private static final Lit i1 = StdLit.fromUnescaped("1", xsdInt);
    private static final Lit i2 = StdLit.fromUnescaped("2", xsdInt);
    private static final Lit i3 = StdLit.fromUnescaped("3", xsdInt);
    private static final Lit i4 = StdLit.fromUnescaped("4", xsdInt);

    private static final Property jOp1 = ResourceFactory.createProperty(op1.getURI());
    private static final Property jResult = ResourceFactory.createProperty(result.getURI());
    private static final Property jTotal = ResourceFactory.createProperty(total.getURI());

    @Path("/")
    public static class Service {
        @GET @Path("square/{x}")
        public @Nonnull Model square(@PathParam("x") int x, @Context UriInfo uriInfo) {
            Model model = ModelFactory.createDefaultModel();
            model.createResource(uriInfo.getAbsolutePath().toString())
                    .addProperty(jOp1, createTypedLiteral(x))
                    .addProperty(jResult, createTypedLiteral(x*x));
            assert model.size() == 2;
            return model;
        }

        @GET @Path("count/{pages}")
        public @Nonnull Model pages(@PathParam("pages") int pages, @QueryParam("p") int p,
                                    @Context UriInfo uriInfo) {
            Model model = ModelFactory.createDefaultModel();
            if (p <= 0) {
                fail(format("pages(pages=%d, p=%d) called! p <= 0", pages, p));
            } else if (p <= pages) {
                model.createResource(uriInfo.getAbsolutePath().toString())
                        .addProperty(jTotal, createTypedLiteral(pages))
                        .addProperty(jResult, createTypedLiteral(p));
            } else if (p > pages+1) {
                fail(format("pages(pages=%d, p=%d) called! p > %d", pages, p, pages+1));
            }
            return model;
        }

        @GET @Path("exp/{base}")
        public @Nonnull Model exp(@PathParam("base") int base, @QueryParam("exp") int exp,
                                  @Context UriInfo uriInfo) {
            if (exp == 0) exp = 2;
            Model model = ModelFactory.createDefaultModel();
            model.createResource(uriInfo.getAbsolutePath().toString())
                    .addProperty(jOp1, createTypedLiteral(base))
                    .addProperty(jResult, createTypedLiteral((int)Math.pow(base, exp)));
            return model;
        }
    }

    private @Nonnull APIMolecule squareMolecule() {
        UriTemplate tpl = new UriTemplate(target().getUri().toString() + "square/{i}");
        UriTemplateExecutor exec = new UriTemplateExecutor(tpl);
        Molecule molecule = Molecule.builder("Square")
                .out(op1, Molecule.builder("op1").buildAtom())
                .out(result, Molecule.builder("result").buildAtom())
                .exclusive()
                .build();
        Map<String, String> atom2in = new HashMap<>();
        atom2in.put("op1", "i");
        return new APIMolecule(molecule, exec, atom2in);
    }

    private @Nonnull APIMolecule countMolecule() {
        UriTemplate tpl = new UriTemplate(target().getUri().toString() + "count/{i}{?p}");
        UriTemplateExecutor exec = UriTemplateExecutor.from(tpl)
                .withPagingStrategy(ParamPagingStrategy.builder("p").build())
                .build();
        Molecule molecule = Molecule.builder("Count")
                .out(total, Molecule.builder("Total").buildAtom())
                .out(result, Molecule.builder("Result").buildAtom())
                .exclusive()
                .build();
        Map<String, String> atom2in = new HashMap<>();
        atom2in.put("Total", "i");
        return new APIMolecule(molecule, exec, atom2in);
    }

    private @Nonnull APIMolecule expMolecule() {
        UriTemplate tpl = new UriTemplate(target().getUri().toString() + "exp/{b}{?exp}");
        UriTemplateExecutor exec = UriTemplateExecutor.from(tpl)
                .withOptional("exp")
                .build();
        assertEquals(exec.getRequiredInputs(), singleton("b"));
        assertEquals(exec.getOptionalInputs(), singleton("exp"));
        Molecule molecule = Molecule.builder("Count")
                .out(op1, Molecule.builder("Operand").buildAtom())
                .out(exponent, Molecule.builder("Exponent").buildAtom())
                .out(result, Molecule.builder("Result").buildAtom())
                .exclusive()
                .build();
        Map<String, String> atom2in = new HashMap<>();
        atom2in.put("Operand", "b");
        atom2in.put("Exponent", "exp");
        return new APIMolecule(molecule, exec, atom2in);
    }


    @Override
    protected @Nonnull Application configure() {
        return new ResourceConfig().register(ModelMessageBodyWriter.class).register(Service.class);
    }

    @Test
    public void selfTestSquare() {
        String json = target("/square/2").request("application/ld+json").get(String.class);
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, new StringReader(json), null, Lang.JSONLD);

        assertEquals(model.size(), 2);
        List<Statement> statements = model.listStatements(null, jResult, (RDFNode) null).toList();
        assertEquals(statements.size(), 1);
        assertTrue(statements.get(0).getSubject().isURIResource());
        assertEquals(statements.get(0).getLiteral(), createTypedLiteral(4));
    }


    @Test
    public void selfTestCount() {
        UriTemplate tpl = new UriTemplate(target().getUri().toString() + "count/{i}{?p}");
        Map<String, String> map = new HashMap<>();
        map.put("i", "3");
        map.put("p", "2");
        String pathAndQuery = tpl.createURI(map).replace(target().getUri().toString(), "");
        assertEquals(pathAndQuery, "count/3?p=2");

        String json = target("count/3").queryParam("p", 2)
                .request("application/ld+json").get(String.class);
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, new StringReader(json), null, Lang.JSONLD);

        assertEquals(model.size(), 2);
        Statement sTotal = model.listStatements(null, jTotal, (RDFNode) null).toList().get(0);
        Statement sResult = model.listStatements(null, jResult, (RDFNode) null).toList().get(0);

        assertEquals(sTotal.getObject(), JenaWrappers.toJena(i3));
        assertEquals(sResult.getObject(), JenaWrappers.toJena(i2));

        String jsonEmpty = target("count/3").queryParam("p", 4)
                .request("application/ld+json").get(String.class);
        Model emptyModel = ModelFactory.createDefaultModel();
        RDFDataMgr.read(emptyModel, new StringReader(jsonEmpty), null, Lang.JSONLD);
        assertEquals(emptyModel.size(), 0);

    }

    @Test
    public void selfTestExp() {
        String jsonld = target("exp/4").queryParam("exp", 3)
                                             .request("application/ld+json").get(String.class);
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, new StringReader(jsonld), null, Lang.JSONLD);

        assertEquals(model.size(), 2);
        List<Statement> list = model.listStatements(null, jOp1, (RDFNode) null).toList();
        assertEquals(list.size(), 1);
        assertEquals(list.get(0).getLiteral(), createTypedLiteral(4));

        list = model.listStatements(null, jResult, (RDFNode) null).toList();
        assertEquals(list.size(), 1);
        assertEquals(list.get(0).getLiteral(), createTypedLiteral(64));
    }

    @Test
    public void testDirectQuery() {
        WebAPICQEndpoint ep = new WebAPICQEndpoint(squareMolecule());
        Results results = ep.query(CQuery.from(new Triple(x, op1, i2),
                                               new Triple(x, result, y)));
        assertTrue(results.hasNext());
        Solution solution = results.next();
        Term x = solution.get(WebAPICQEndpointTest.x.getName());
        assertNotNull(x);
        assertTrue(x.isURI());
        assertEquals(solution.get(y.getName()), i4);
    }

    @Test
    public void testMatchThenQuery() {
        WebAPICQEndpoint ep = new WebAPICQEndpoint(squareMolecule());

        APIMoleculeMatcher matcher = new APIMoleculeMatcher(ep.getMolecule());
        CQuery query = CQuery.from(new Triple(x, op1, i2),
                                   new Triple(x, result, y));
        CQueryMatch match = matcher.match(query);
        assertEquals(match.getKnownExclusiveGroups().size(), 1);

        Results results = ep.query(match.getKnownExclusiveGroups().get(0));
        assertTrue(results.hasNext());
        Solution solution = results.next();
        Term x = solution.get(WebAPICQEndpointTest.x.getName());
        assertNotNull(x);
        assertTrue(x.isURI());
        assertEquals(solution.get(y.getName()), i4);
    }

    @Test
    public void testMatchThenConsumePaged() {
        WebAPICQEndpoint ep = new WebAPICQEndpoint(countMolecule());

        APIMoleculeMatcher matcher = new APIMoleculeMatcher(ep.getMolecule());
        CQuery query = CQuery.from(new Triple(x, total, i3),
                                   new Triple(x, result, y));
        CQueryMatch match = matcher.match(query);
        assertEquals(match.getKnownExclusiveGroups().size(), 1);

        Atom Total = ep.getMolecule().getMolecule().getAtomMap().get("Total");
        assertEquals(match.getKnownExclusiveGroups().get(0).getTermAnnotations(i3),
                     singletonList(AtomInputAnnotation.asRequired(Total, "i").get()));

        Results results = ep.query(match.getKnownExclusiveGroups().get(0));
        List<Term> values = new ArrayList<>(), expected = asList(i1, i2, i3);
        results.forEachRemainingThenClose(s -> values.add(s.get(y)));
        assertEquals(values, expected); //ordered due to paging
    }

    @Test
    public void testDirectConsumePaged() {
        WebAPICQEndpoint ep = new WebAPICQEndpoint(countMolecule());
        CQuery query = CQuery.from(new Triple(x, total, i3),
                                   new Triple(x, result, y));
        Results results = ep.query(query);
        List<Term> values = new ArrayList<>(), expected = asList(i1, i2, i3);
        results.forEachRemainingThenClose(s -> values.add(s.get(y)));
        assertEquals(values, expected); //ordered due to paging
    }

    @Test
    public void testDirectQueryDescriptiveTriple() {
        WebAPICQEndpoint ep = new WebAPICQEndpoint(expMolecule());
        CQuery query = createQuery(x, exponent, i3, PureDescriptive.INSTANCE,
                                   x, op1, i4,
                                   x, result, y);
        List<Term> list = new ArrayList<>();
        ep.query(query).forEachRemainingThenClose(s -> list.add(s.get(y)));
        assertEquals(list, singletonList(fromJena(createTypedLiteral(64))));
    }
}