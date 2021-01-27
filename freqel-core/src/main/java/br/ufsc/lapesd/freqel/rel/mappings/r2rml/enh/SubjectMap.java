package br.ufsc.lapesd.freqel.rel.mappings.r2rml.enh;

import org.apache.jena.rdf.model.Resource;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.stream.Stream;

public interface SubjectMap extends TermMap {
    @Nonnull Iterator<Resource> getClassesIterator();
    @Nonnull Iterable<Resource> getClasses();
    @Nonnull Stream<Resource> streamClasses();
}
