package br.ufsc.lapesd.riefederator.reason.tbox;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.owlapi.model.OWLAPITerm;
import br.ufsc.lapesd.riefederator.owlapi.model.OWLAPITermFactory;
import com.google.common.base.Preconditions;
import org.semanticweb.HermiT.ReasonerFactory;
import org.semanticweb.owlapi.model.AsOWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObject;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLRuntimeException;
import org.semanticweb.owlapi.reasoner.InferenceType;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.OWLReasonerFactory;
import org.semanticweb.owlapi.reasoner.structural.StructuralReasonerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.manchester.cs.jfact.JFactFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * Adaptor for using owlapi reasoners.
 */
public class OWLAPITBoxReasoner implements TBoxReasoner {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(OWLAPITBoxReasoner.class);
    private @Nonnull OWLReasonerFactory factory;
    private final boolean ignoreExceptionOnStreamClosure;
    private @Nullable OWLReasoner reasoner;
    private @Nullable OWLAPITermFactory termFactory;

    public OWLAPITBoxReasoner(@Nonnull OWLReasonerFactory factory) {
        this(factory, false);
    }

    public OWLAPITBoxReasoner(@Nonnull OWLReasonerFactory factory,
                              boolean ignoreExceptionOnStreamClosure) {
        this.factory = factory;
        this.ignoreExceptionOnStreamClosure = ignoreExceptionOnStreamClosure;
    }

    /** Uses HermiT through owlapi. */
    public static @Nonnull
    OWLAPITBoxReasoner hermit() {
        return new OWLAPITBoxReasoner(new ReasonerFactory());
    }

    /** Uses owlapi's strutural reasoner. It does no reasoning, not even transitivity. */
    public static @Nonnull
    OWLAPITBoxReasoner structural() {
        return new OWLAPITBoxReasoner(new StructuralReasonerFactory());
    }

    /** Uses JFact reasoner. */
    public static @Nonnull
    OWLAPITBoxReasoner jFact() {
        return new OWLAPITBoxReasoner(new JFactFactory(), true);
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

    private @Nonnull <T extends OWLObject>
    Stream<Term> streamClosure(@Nullable T s, @Nonnull BiFunction<T, Boolean, Stream<T>> getter) {
        try {
            if (s != null) return getter.apply(s, false).map(OWLAPITerm::wrap);
        } catch (OWLRuntimeException e) {
            if (!ignoreExceptionOnStreamClosure)
                throw e;
            logger.debug("streamClosure({}, {}) threw (this is somewhat expected)", s, getter, e);
        }
        return Stream.empty();
    }

    @Override
    public @Nonnull Stream<Term> subClasses(@Nonnull Term term) {
        Preconditions.checkState(reasoner != null, "Reasoner not loaded");
        assert termFactory != null;
        if (!term.isRes()) {
            logger.warn("Suspicious subClasses({}) call -- term is not URI nor blank", term);
            return Stream.empty(); // only URI and blank nodes can be classes
        }
        return streamClosure(termFactory.convertToOWLClass(term), reasoner::subClasses);
    }

    @Override
    public @Nonnull Stream<Term> subProperties(@Nonnull Term term) {
        Preconditions.checkState(reasoner != null, "Reasoner not loaded");
        assert termFactory != null;
        if (!term.isURI()) {
            logger.warn("Suspicious subProperties({}) call -- term is not an URI", term);
            return Stream.empty(); // only URIs can be predicates
        }
        return Stream.concat(
                streamClosure(termFactory.convertToOWLObjectProperty(term),
                        (s, d) -> reasoner.subObjectProperties(s, d)
                                  .filter(AsOWLObjectProperty::isOWLObjectProperty)
                                  .map(AsOWLObjectProperty::asOWLObjectProperty)),
                streamClosure(termFactory.convertToOWLDataProperty(term),
                              reasoner::subDataProperties)
        );
    }

    @Override
    public void close() {
        // no-op
    }
}
