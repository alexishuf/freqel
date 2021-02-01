package br.ufsc.lapesd.freqel.query.parse;


import javax.annotation.Nonnull;

public class UnsupportedSPARQLFeatureException extends SPARQLParseException {
    public UnsupportedSPARQLFeatureException(@Nonnull String message, @Nonnull String query) {
        super(message, query);
    }

    public UnsupportedSPARQLFeatureException(@Nonnull String message, @Nonnull Throwable cause,
                                             @Nonnull String query) {
        super(message, cause, query);
    }
}
