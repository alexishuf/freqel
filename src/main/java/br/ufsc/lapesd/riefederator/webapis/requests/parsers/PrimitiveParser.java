package br.ufsc.lapesd.riefederator.webapis.requests.parsers;

import org.apache.jena.rdf.model.RDFNode;

import javax.annotation.Nullable;

public interface PrimitiveParser {
    @Nullable RDFNode parse(@Nullable String value);
}
