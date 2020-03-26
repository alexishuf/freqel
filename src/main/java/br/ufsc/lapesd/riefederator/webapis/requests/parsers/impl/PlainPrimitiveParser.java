package br.ufsc.lapesd.riefederator.webapis.requests.parsers.impl;

import br.ufsc.lapesd.riefederator.webapis.requests.parsers.PrimitiveParser;
import com.google.errorprone.annotations.Immutable;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Immutable
public final class PlainPrimitiveParser implements PrimitiveParser {
    public static final @Nonnull PlainPrimitiveParser INSTANCE = new PlainPrimitiveParser();

    @Override
    public @Nullable RDFNode parse(@Nullable String value) {
        if (value == null) return null;
        return ResourceFactory.createPlainLiteral(value);
    }

    @Override
    public String toString() {
        return "PlainPrimitiveParser";
    }

    @Override
    public int hashCode() {
        return 53;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PlainPrimitiveParser;
    }
}
