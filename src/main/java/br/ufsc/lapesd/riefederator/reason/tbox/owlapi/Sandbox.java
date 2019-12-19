package br.ufsc.lapesd.riefederator.reason.tbox.owlapi;

import com.google.common.base.Stopwatch;
import org.semanticweb.HermiT.ReasonerFactory;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.InferenceType;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Sandbox {

    public static void main(String[] args) throws Exception {
        OWLOntologyManager mgr = OWLManager.createOWLOntologyManager();
        mgr.createOntology(IRI.create("http://www.w3.org/2002/07/owl"));
        InputStream stream = new FileInputStream("/tmp/onto-1.ttl");
        OWLOntology onto = mgr.loadOntologyFromOntologyDocument(stream);
        Stopwatch sw = Stopwatch.createStarted();
        OWLReasoner reasoner = new ReasonerFactory().createReasoner(onto);
        System.out.printf("reasoner created in %.3fms.\n", sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
        sw.reset().start();

        reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
        reasoner.precomputeInferences(InferenceType.OBJECT_PROPERTY_HIERARCHY);
        reasoner.precomputeInferences(InferenceType.DATA_PROPERTY_HIERARCHY);
        System.out.printf("precomputeInferences in %.3fms.\n", sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
        sw.reset().start();

        OWLClass thingy = mgr.getOWLDataFactory().getOWLClass("http://example.org/Thingy");
        OWLClass a = mgr.getOWLDataFactory().getOWLClass("http://example.org/A");
        OWLClass b = mgr.getOWLDataFactory().getOWLClass("http://example.org/B");
        System.out.printf("created OWLClass instances in %.3fms.\n", sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
        sw.reset().start();

        for (int i = 0; i < 3; i++) {
            System.out.println(reasoner.getSubClasses(thingy, false).entities().map(Object::toString).collect(Collectors.joining(", ")));
            System.out.printf("listed subclasses of Thingy in %.3fms.\n", sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
            sw.reset().start();

            System.out.println(reasoner.getSubClasses(a, false).entities().map(Object::toString).collect(Collectors.joining(", ")));
            System.out.printf("listed subclasses of A in %.3fms.\n", sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
            sw.reset().start();

            System.out.println(reasoner.getSubClasses(b, false).entities().map(Object::toString).collect(Collectors.joining(", ")));
            System.out.printf("listed subclasses of B in %.3fms.\n", sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
            sw.reset().start();
        }
    }
}
