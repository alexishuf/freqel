package br.ufsc.lapesd.riefederator.model.term.std;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.URI;
import org.testng.annotations.Test;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromURIResource;
import static br.ufsc.lapesd.riefederator.query.parse.CQueryContext.createQuery;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TemplateLinkTest implements TestContext {

    @SuppressWarnings("SimplifiedTestNGAssertion")
    @Test
    public void testCompareURI() {
        TemplateLink link = new TemplateLink(EX + "hasThing", createQuery(x, author, y), x, y);
        URI stdURI = new StdURI(EX + "hasThing");
        URI jenaURI = fromURIResource(createResource(EX + "hasThing"));

        assertEquals(link, stdURI);
        assertTrue(link.equals(stdURI));
        assertTrue(stdURI.equals(link));
        assertEquals(link, jenaURI);
        assertTrue(link.equals(jenaURI));
        assertTrue(jenaURI.equals(link));
    }

    @SuppressWarnings("SimplifiedTestNGAssertion")
    @Test
    public void testComparePlain() {
        TemplateLink link = new TemplateLink(StdPlain.URI_PREFIX + "hasThing",
                createQuery(x, author, y), x, y);
        URI stdURI = new StdURI(StdPlain.URI_PREFIX + "hasThing");
        URI jenaURI = fromURIResource(createResource(StdPlain.URI_PREFIX + "hasThing"));
        URI plain = new StdPlain("hasThing");

        assertEquals(link, stdURI);
        assertTrue(link.equals(stdURI));
        assertTrue(stdURI.equals(link));
        assertEquals(link, jenaURI);
        assertTrue(link.equals(jenaURI));
        assertTrue(jenaURI.equals(link));

        assertEquals(link, plain);
        assertTrue(link.equals(plain));
        assertTrue(plain.equals(link));

    }
}