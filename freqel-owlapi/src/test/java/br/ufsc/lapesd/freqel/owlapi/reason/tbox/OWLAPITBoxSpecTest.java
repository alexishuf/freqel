package br.ufsc.lapesd.freqel.owlapi.reason.tbox;

import br.ufsc.lapesd.freqel.jena.ModelUtils;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import br.ufsc.lapesd.freqel.util.ExtractedResource;
import com.github.lapesd.rdfit.source.RDFInputStream;
import com.github.lapesd.rdfit.source.syntax.RDFLangs;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.OWL2;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.structural.StructuralReasonerFactory;
import org.semanticweb.owlapi.util.SimpleIRIMapper;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.apache.jena.rdf.model.ResourceFactory.createResource;
import static org.testng.Assert.*;

@Test(groups = {"fast"})
public class OWLAPITBoxSpecTest {
    private TBoxSpec fullSpec;
    private ExtractedResource provo, time;
    private static final String TIME_URI = "http://www.w3.org/2006/time";

    @BeforeMethod
    public void setUp() throws Exception {
        Model onto1, onto2;
        onto1 = new TBoxSpec().fetchOwlImports(false).addResource("onto-1.ttl").loadModel();
        onto2 = new TBoxSpec().fetchOwlImports(false).addResource("onto-2.ttl").loadModel();
        InputStream onto3 = getClass().getResourceAsStream("../../onto-3.ttl");
        assertNotNull(onto3);
        fullSpec = new TBoxSpec()
                .add(onto1)
                .add(ModelUtils.toTemp(onto2, false))
                .add(new RDFInputStream(onto3, RDFLangs.TTL))
                .addResource(OWLAPITBoxSpecTest.class, "../../onto-4.ttl")
                .add(TIME_URI);

        provo = new ExtractedResource(getClass(), "../../prov-o.ttl");
        time = new ExtractedResource(getClass(), "../../time.ttl");
    }

    @AfterMethod
    public void tearDown() {
        fullSpec.close();
        provo.close();
        time.close();
    }

    @Test
    public void testLoadToOWLOntology() {
        OWLAPITBoxSpec oaSpec = new OWLAPITBoxSpec(fullSpec);
        OWLOntologyManager mgr = oaSpec.createOwlOntologyManager();
        // the following block is only here to real HTTP gets everytime the test runs
        Set<OWLOntologyIRIMapper> mappers = new HashSet<>();
        mgr.getIRIMappers().forEach(mappers::add);
        mappers.add(new SimpleIRIMapper(IRI.create("http://www.w3.org/ns/prov-o"),
                                        IRI.create(provo)));
        mappers.add(new SimpleIRIMapper(IRI.create(TIME_URI),
                                        IRI.create(time)));
        mgr.setIRIMappers(mappers);

        //now for the real deal...
        OWLOntology onto = oaSpec.loadOWLOntology(mgr);
        assertNotNull(onto);
        OWLReasoner reasoner = new StructuralReasonerFactory().createReasoner(onto);
        OWLDataFactory df = onto.getOWLOntologyManager().getOWLDataFactory();
        OWLClass thingy = df.getOWLClass("http://example.org/onto-1.ttl#Thingy");
        OWLClass thing = df.getOWLClass(OWL2.Thing.getURI());
        OWLClass a1 = df.getOWLClass("http://example.org/onto-2.ttl#A1");
        OWLClass a = df.getOWLClass("http://example.org/onto-2.ttl#A");
        OWLClass b1 = df.getOWLClass("http://example.org/onto-3.ttl#B1");
        OWLClass b = df.getOWLClass("http://example.org/onto-3.ttl#B");
        OWLClass c1 = df.getOWLClass("http://example.org/onto-4.ttl#C1");
        OWLClass c = df.getOWLClass("http://example.org/onto-4.ttl#C");
        OWLClass interval = df.getOWLClass("http://www.w3.org/2006/time#Interval");
        OWLClass proper = df.getOWLClass("http://www.w3.org/2006/time#ProperInterval");
        OWLClass collection = df.getOWLClass("http://www.w3.org/ns/prov#Collection");
        OWLClass entity = df.getOWLClass("http://www.w3.org/ns/prov#Entity");

        assertTrue(reasoner.subClasses(thing, false).collect(toSet()).contains(thingy));
        assertTrue(reasoner.subClasses(a, false).collect(toSet()).contains(a1));
        assertTrue(reasoner.subClasses(b, false).collect(toSet()).contains(b1));
        assertTrue(reasoner.subClasses(c, false).collect(toSet()).contains(c1));
        assertTrue(reasoner.subClasses(entity, false).collect(toSet()).contains(collection));
        assertTrue(reasoner.subClasses(interval, false).collect(toSet()).contains(proper));
    }
}