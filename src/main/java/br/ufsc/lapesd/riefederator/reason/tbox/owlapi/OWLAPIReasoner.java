package br.ufsc.lapesd.riefederator.reason.tbox.owlapi;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.owlapi.model.OWLAPITerm;
import br.ufsc.lapesd.riefederator.owlapi.model.OWLAPITermFactory;
import br.ufsc.lapesd.riefederator.reason.tbox.Reasoner;
import br.ufsc.lapesd.riefederator.reason.tbox.TBoxSpec;
import com.google.common.base.Preconditions;
import org.semanticweb.HermiT.ReasonerFactory;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.InferenceType;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.OWLReasonerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.stream.Stream;

/**
 * Adaptor for using owlapi reasoners.
 */
public class OWLAPIReasoner implements Reasoner {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(OWLAPIReasoner.class);
    private @Nonnull OWLReasonerFactory factory;
    private @Nullable OWLReasoner reasoner;
    private @Nullable OWLAPITermFactory termFactory;

    public OWLAPIReasoner(@Nonnull OWLReasonerFactory factory) {
        this.factory = factory;
    }

    public static @Nonnull OWLAPIReasoner hermitReasoner() {
        return new OWLAPIReasoner(new ReasonerFactory());
    }

    @Override
    public void load(@Nonnull TBoxSpec sources) {
        OWLOntology onto = sources.loadOWLOntology();
        termFactory = new OWLAPITermFactory(onto.getOWLOntologyManager().getOWLDataFactory());
        reasoner = factory.createReasoner(onto);
        reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
        reasoner.precomputeInferences(InferenceType.OBJECT_PROPERTY_HIERARCHY);
        reasoner.precomputeInferences(InferenceType.DATA_PROPERTY_HIERARCHY);
    }

    @Override
    public @Nonnull Stream<Term> subClasses(@Nonnull Term term) {
        Preconditions.checkState(reasoner != null, "Reasoner not loaded");
        assert termFactory != null;
        if (!term.isRes()) {
            logger.warn("Supicious subClasses({}) call -- term is not URI nor blank", term);
            return Stream.empty(); // only URI and blank nodes can be classes
        }
        OWLAPITerm converted = (OWLAPITerm) termFactory.convert(term);
        return reasoner.subClasses(converted.asOWLClass(), false).map(OWLAPITerm::wrap);
    }

    @Override
    public @Nonnull Stream<Term> subProperties(@Nonnull Term term) {
        Preconditions.checkState(reasoner != null, "Reasoner not loaded");
        assert termFactory != null;
        if (!term.isURI()) {
            logger.warn("Suspicious subProperties({}) call -- term is not an URI", term);
            return Stream.empty(); // only URIs can be predicates
        }
        OWLAPITerm converted = (OWLAPITerm) termFactory.convert(term);
        OWLProperty prop = converted.asOWLProperty();
        if (prop instanceof OWLObjectProperty) {
            OWLObjectProperty p = (OWLObjectProperty) converted.asOWLProperty();
            return reasoner.subObjectProperties(p, false).map(OWLAPITerm::wrap);
        } else if (prop instanceof OWLDataProperty) {
            OWLDataProperty p = (OWLDataProperty) converted.asOWLProperty();
            return reasoner.subDataProperties(p, false).map(OWLAPITerm::wrap);
        } else if (prop instanceof OWLAnnotationProperty) {
            return Stream.empty();
        }
        logger.error("Unknown OWLProperty type for {}", term);
        return Stream.empty();
    }

    @Override
    public void close() {
        // no-op
    }
}
