package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.V;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.URI;

import javax.annotation.Nonnull;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Arrays.asList;

/**
 * Tests wheter a {@link Triple} is part of the to the TBox
 */
public class IsTBoxTriple implements Predicate<Triple> {
    private static final @Nonnull Set<String> PREDICATES;
    private static final @Nonnull Set<String> CLASSES;
    public static final @Nonnull IsTBoxTriple INSTANCE = new IsTBoxTriple();

    static {
        Set<String> predicates = new HashSet<>();
        predicates.add(V.RDFS.domain.getURI());
        predicates.add(V.RDFS.range.getURI());
        predicates.add(V.RDFS.subClassOf.getURI());
        predicates.add(V.RDFS.subPropertyOf.getURI());
        Set<String> classes = new HashSet<>();
        classes.add(V.RDFS.Class.getURI());
        classes.add(V.RDF.Property.getURI());
        Set<String> notTBoxPredicates = new HashSet<>(asList(
                V.OWL.differentFrom.getURI(),
                V.OWL.sameAs.getURI(),
                V.OWL.topObjectProperty.getURI(),
                V.OWL.topDataProperty.getURI(),
                V.OWL.bottomObjectProperty.getURI(),
                V.OWL.bottomDataProperty.getURI()));
        Set<String> notTBoxClasses = new HashSet<>(asList(
                V.OWL.Thing.getURI(),
                V.OWL.Nothing.getURI()));
        for (Field field : V.OWL.class.getDeclaredFields()) {
            if (!Modifier.isStatic(field.getModifiers())) continue;
            if (!URI.class.isAssignableFrom(field.getType())) continue;

            String uri;
            try {
                uri = ((URI) field.get(null)).getURI();
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Cannot read fields of "+V.OWL.class, e);
            }
            if (Character.isLowerCase(field.getName().charAt(0))) {
                if (!notTBoxPredicates.contains(uri))
                    predicates.add(uri);
            } else {
                if (!notTBoxClasses.contains(uri))
                    classes.add(uri);
            }
        }
        PREDICATES = Collections.unmodifiableSet(predicates);
        CLASSES = Collections.unmodifiableSet(classes);
    }

    @Override public boolean test(@Nonnull Triple triple) {
        Term p = triple.getPredicate(), o = triple.getObject();
        if (V.RDF.type.equals(p))
            return o.isURI() && CLASSES.contains(o.asURI().getURI());
        return p.isURI() && PREDICATES.contains(p.asURI().getURI());
    }
}
