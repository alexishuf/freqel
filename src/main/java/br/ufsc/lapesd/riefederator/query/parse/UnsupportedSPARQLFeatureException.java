package br.ufsc.lapesd.riefederator.query.parse;

import org.apache.jena.query.Query;

import javax.annotation.Nonnull;

public class UnsupportedSPARQLFeatureException extends SPARQLParseException {
    public UnsupportedSPARQLFeatureException(@Nonnull String message, @Nonnull String query) {
        super(message, query);
    }

    public UnsupportedSPARQLFeatureException(@Nonnull String message, @Nonnull Query query) {
        super(message, query.serialize());
    }

    public UnsupportedSPARQLFeatureException(@Nonnull String message, @Nonnull Throwable cause,
                                             @Nonnull String query) {
        super(message, cause, query);
    }
}
