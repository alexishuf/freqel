package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.description.CQueryMatch;
import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Lit;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.URI;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Results;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.webapis.description.APIMolecule;
import br.ufsc.lapesd.riefederator.webapis.description.APIMoleculeMatcher;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.ModelMessageBodyWriter;
import br.ufsc.lapesd.riefederator.webapis.requests.impl.UriTemplateExecutor;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.XSD;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTestNg;
import org.glassfish.jersey.uri.UriTemplate;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.jena.rdf.model.ResourceFactory.createTypedLiteral;
import static org.testng.Assert.*;

public class WebAPICQEndpointTest extends JerseyTestNg.ContainerPerClassTest {
    private static final URI xint = new StdURI(XSD.xint.getURI());
    private static final URI op1 = new StdURI("http://example.org/op1");
    private static final URI result = new StdURI("http://example.org/result");
    private static final Lit i2 = StdLit.fromUnescaped("2", xint);
    private static final Lit i4 = StdLit.fromUnescaped("4", xint);
    private static final Var X = new StdVar("x");
    private static final Var Y = new StdVar("y");

    private static final Property jOp1 = ResourceFactory.createProperty(op1.getURI());
    private static final Property jResult = ResourceFactory.createProperty(result.getURI());

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
    public void testDirectQuery() {
        WebAPICQEndpoint ep = new WebAPICQEndpoint(squareMolecule());
        Results results = ep.query(CQuery.from(new Triple(X, op1, i2),
                                               new Triple(X, result, Y)));
        assertTrue(results.hasNext());
        Solution solution = results.next();
        Term x = solution.get(X.getName());
        assertNotNull(x);
        assertTrue(x.isURI());
        assertEquals(solution.get(Y.getName()), i4);
    }

    @Test
    public void testMatchThenQuery() {
        WebAPICQEndpoint ep = new WebAPICQEndpoint(squareMolecule());

        APIMoleculeMatcher matcher = new APIMoleculeMatcher(ep.getMolecule());
        CQuery query = CQuery.from(new Triple(X, op1, i2),
                                   new Triple(X, result, Y));
        CQueryMatch match = matcher.match(query);
        assertEquals(match.getKnownExclusiveGroups().size(), 1);

        Results results = ep.query(match.getKnownExclusiveGroups().get(0));
        assertTrue(results.hasNext());
        Solution solution = results.next();
        Term x = solution.get(X.getName());
        assertNotNull(x);
        assertTrue(x.isURI());
        assertEquals(solution.get(Y.getName()), i4);
    }
}