package br.ufsc.lapesd.riefederator.rel.mappings.r2rml.enh;

import org.apache.jena.rdf.model.Resource;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.stream.Stream;

public interface TriplesMap extends RRResource {
    @Nonnull SubjectMap getSubjectMap();
    @Nonnull Iterator<PredicateObjectMap> getPredicateObjectMapIterator();
    @Nonnull Iterable<PredicateObjectMap> getPredicateObjectMap();
    @Nonnull Stream<PredicateObjectMap> streamPredicateObjectMap();
    @Nonnull LogicalTable getLogicalTable();

    /**
     * Streams over the classes that are defined by rr:class and by constant-bound
     * predicate-object maps of this triples map.
     *
     * @return possibly empty stream of distinct, non-null URI resources.
     */
    @Nonnull Stream<Resource> streamClasses();
}
