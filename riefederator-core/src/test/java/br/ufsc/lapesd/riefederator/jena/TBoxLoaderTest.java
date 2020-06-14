package br.ufsc.lapesd.riefederator.jena;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static br.ufsc.lapesd.riefederator.jena.ModelUtils.list;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = {"fast"})
public class TBoxLoaderTest {

    @Test
    public void testLoadLanguage() {
        Model model = new TBoxLoader().addOWL().addRDF().getModel();

        Set<Resource> ontologies = list(model, null, RDF.type, OWL2.Ontology,
                Statement::getSubject).collect(Collectors.toSet());
        Set<Resource> expectedOntologies = new HashSet<>(Arrays.asList(
                createResource(RDF.getURI()),
                createResource(RDFS.getURI()),
                createResource(OWL2.getURI().replaceAll("#$", ""))));
        assertEquals(ontologies, expectedOntologies);
    }

    @Test
    public void testUseResourceInsteadOfURI() {
        Model model = ModelFactory.createDefaultModel();
        // This URI is not resolvable
        String onto2URI = "http://example.org/onto-2.ttl";
        String onto3URI = "http://example.org/onto-3.ttl";
        model.createResource("http://example.org/onto.ttl")
                .addProperty(RDF.type, OWL2.Ontology)
                .addProperty(OWL2.imports, createResource(onto2URI+"#"))
                .addProperty(OWL2.imports, createResource(onto3URI));
        Model loaded = new TBoxLoader()
                .mapping(onto2URI, "/br/ufsc/lapesd/riefederator/onto-2.ttl")
                .mapping(onto3URI+"#", getClass(), "../onto-3.ttl")
                .addModel(model).getModel();

        Resource a = createResource(onto2URI + "#A");
        Resource a1 = createResource(onto2URI + "#A1");
        Resource b = createResource(onto3URI + "#B");
        Resource b1 = createResource(onto3URI + "#B1");

        assertTrue(list(model, null, null, null).allMatch(loaded::contains));
        // loaded onto-2.ttl from URI map
        assertTrue(loaded.contains(a1, RDFS.subClassOf, a));
        // loaded onto-2.ttl from URI map
        assertTrue(loaded.contains(b1, RDFS.subClassOf, b));
    }

    @Test
    public void testLoadOWLByURI() {
        Model model = new TBoxLoader().addFromResource("onto-owl.ttl").getModel();

        Set<Resource> ontologies = list(model, null, RDF.type, OWL2.Ontology,
                Statement::getSubject).collect(Collectors.toSet());
        HashSet<Resource> expected = new HashSet<>(Arrays.asList(
                createResource("http://example.org/onto-owl.ttl#onto"),
                createResource(RDFS.getURI()),
                createResource("http://www.w3.org/2002/07/owl")));
        assertEquals(ontologies, expected);
    }

    @Test
    public void testLoadSKOS() {
        String uri = "http://www.w3.org/2004/02/skos/core#";
        Model model = new TBoxLoader().addRDF().fetchOntology(uri).getModel();
        Set<Resource> ontologies = list(model, null, RDF.type, OWL2.Ontology,
                Statement::getSubject).collect(Collectors.toSet());
        // SKOS makes no imports
        Set<Resource> expectedOntologies = new HashSet<>(Arrays.asList(
                createResource(RDF.getURI()),
                createResource(uri.replaceAll("#$", ""))));
        assertEquals(ontologies, expectedOntologies);
    }
}