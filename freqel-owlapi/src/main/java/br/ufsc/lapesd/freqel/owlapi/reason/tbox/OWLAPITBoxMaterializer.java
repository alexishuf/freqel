package br.ufsc.lapesd.freqel.owlapi.reason.tbox;

import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.owlapi.model.OWLAPITerm;
import br.ufsc.lapesd.freqel.owlapi.model.OWLAPITermFactory;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxMaterializer;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import org.semanticweb.HermiT.ReasonerFactory;
import org.semanticweb.owlapi.model.*;
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
public class OWLAPITBoxMaterializer implements TBoxMaterializer {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(OWLAPITBoxMaterializer.class);

    private @Nonnull final OWLReasonerFactory factory;
    private final boolean ignoreExceptionOnStreamClosure;
    private @Nullable OWLReasoner reasoner;
    private @Nullable OWLAPITermFactory termFactory;
    private boolean warnedNotLoaded = false;

    public OWLAPITBoxMaterializer(@Nonnull OWLReasonerFactory factory) {
        this(factory, false);
    }

    public OWLAPITBoxMaterializer(@Nonnull OWLReasonerFactory factory,
                                  boolean ignoreExceptionOnStreamClosure) {
        this.factory = factory;
        this.ignoreExceptionOnStreamClosure = ignoreExceptionOnStreamClosure;
    }

    /** Uses HermiT through owlapi. */
    public static @Nonnull OWLAPITBoxMaterializer hermit() {
        return new OWLAPITBoxMaterializer(new ReasonerFactory());
    }

    /** Uses owlapi's strutural reasoner. It does no reasoning, not even transitivity. */
    public static @Nonnull OWLAPITBoxMaterializer structural() {
        return new OWLAPITBoxMaterializer(new StructuralReasonerFactory());
    }

    /** Uses JFact reasoner. */
    public static @Nonnull OWLAPITBoxMaterializer jFact() {
        return new OWLAPITBoxMaterializer(new JFactFactory(), true);
    }

    @Override
    public void load(@Nonnull TBoxSpec sources) {
        OWLOntology onto = new OWLAPITBoxSpec(sources).loadOWLOntology();
        termFactory = new OWLAPITermFactory(onto.getOWLOntologyManager().getOWLDataFactory());
        reasoner = factory.createReasoner(onto);
        reasoner.precomputeInferences(InferenceType.CLASS_HIERARCHY);
        reasoner.precomputeInferences(InferenceType.OBJECT_PROPERTY_HIERARCHY);
        reasoner.precomputeInferences(InferenceType.DATA_PROPERTY_HIERARCHY);
    }

    private void warnNotLoaded() {
        if (!warnedNotLoaded) {
            warnedNotLoaded = true;
            logger.warn("Reasoner is not loaded, will return an empty stream");
        }
    }

    @Override public @Nullable TPEndpoint getEndpoint() {
        return null;
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
        if (reasoner == null) {
            warnNotLoaded();
            return Stream.empty();
        }
        assert termFactory != null;
        if (!term.isRes()) {
            logger.warn("Suspicious subClasses({}) call -- term is not URI nor blank", term);
            return Stream.empty(); // only URI and blank nodes can be classes
        }
        return streamClosure(termFactory.convertToOWLClass(term), reasoner::subClasses);
    }

    @Override public @Nonnull Stream<Term> withSubClasses(@Nonnull Term term) {
        return Stream.concat(Stream.of(term), subClasses(term));
    }

    @Override public boolean isSubClass(@Nonnull Term subClass, @Nonnull Term superClass) {
        if (subClass.equals(superClass))
            return true;
        if (reasoner == null) {
            warnNotLoaded();
            return false;
        }
        if (!subClass.isRes()) {
            logger.warn("Suspicious isSubClass({}, {}) call: {} is not resource.",
                        subClass, superClass, subClass);
            return false;
        }
        if (!superClass.isRes()) {
            logger.warn("Suspicious isSubClass({}, {}) call: {} is not a resource.",
                        subClass, superClass, superClass);
            return false;
        }
        assert termFactory != null;
        OWLClass owlSuperClass = termFactory.convertToOWLClass(superClass);
        if (owlSuperClass == null) {
            logger.debug("superClass={} has no representation as an OWLClass", superClass);
            return false;
        }
        OWLClass owlSubClass = termFactory.convertToOWLClass(subClass);
        if (owlSubClass == null) {
            logger.debug("subClass={} has no representation as an OWLClass", subClass);
            return false;
        }
        return reasoner.getSubClasses(owlSuperClass).containsEntity(owlSubClass);
    }

    @Override
    public @Nonnull Stream<Term> subProperties(@Nonnull Term term) {
        if (reasoner == null) {
            warnNotLoaded();
            return Stream.empty();
        }
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

    @Override public @Nonnull Stream<Term> withSubProperties(@Nonnull Term term) {
        return Stream.concat(Stream.of(term), subProperties(term));
    }

    @Override public boolean isSubProperty(@Nonnull Term subProperty, @Nonnull Term superProperty) {
        if (subProperty.equals(superProperty))
            return true;
        if (reasoner == null) {
            warnNotLoaded();
            return false;
        }
        if (!subProperty.isURI()) {
            logger.warn("Suspicious isSubProperty({}, {}) call: {} is not an IRI", subProperty,
                        superProperty, subProperty);
            return false;
        }
        if (!superProperty.isURI()) {
            logger.warn("Suspicious isSubProperty({}, {}) call: {} is not an IRI", subProperty,
                        superProperty, superProperty);
            return false;
        }
        assert termFactory != null;
        OWLObjectProperty superObjProp = termFactory.convertToOWLObjectProperty(superProperty);
        OWLObjectProperty subObjProp = termFactory.convertToOWLObjectProperty(subProperty);
        OWLDataProperty superDataProp = termFactory.convertToOWLDataProperty(superProperty);
        OWLDataProperty subDataProp = termFactory.convertToOWLDataProperty(subProperty);

        if (superObjProp != null && subObjProp != null)
            return reasoner.getSubObjectProperties(superObjProp).containsEntity(subObjProp);
        if (superDataProp != null && subDataProp != null)
            return reasoner.getSubDataProperties(superDataProp).containsEntity(subDataProp);
        logger.debug("Super and sub property do not match objec/data nature");
        return false;
    }

    @Override
    public void close() {
        // no-op
    }
}
