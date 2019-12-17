package br.ufsc.lapesd.riefederator.jena;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
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
import static org.testng.Assert.assertEquals;

public class TBoxLoaderTest {

    @Test
    public void testLoadLanguage() {
        Model model = new TBoxLoader().addOWL().addRDF().getModel();

        Set<Resource> ontologies = list(model, null, RDF.type, OWL2.Ontology,
                Statement::getSubject).collect(Collectors.toSet());
        Set<Resource> expectedOntologies = new HashSet<>(Arrays.asList(
                ResourceFactory.createResource(RDF.getURI()),
                ResourceFactory.createResource(RDFS.getURI()),
                ResourceFactory.createResource(OWL2.getURI().replaceAll("#$", ""))));
        assertEquals(ontologies, expectedOntologies);
    }

    @Test
    public void testLoadSKOS() {
        String uri = "http://www.w3.org/2004/02/skos/core#";
        Model model = new TBoxLoader().addRDF().fetchOntology(uri).getModel();
        Set<Resource> ontologies = list(model, null, RDF.type, OWL2.Ontology,
                Statement::getSubject).collect(Collectors.toSet());
        // SKOS makes no imports
        Set<Resource> expectedOntologies = new HashSet<>(Arrays.asList(
                ResourceFactory.createResource(RDF.getURI()),
                ResourceFactory.createResource(uri.replaceAll("#$", ""))));
        assertEquals(ontologies, expectedOntologies);
    }
}