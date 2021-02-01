package br.ufsc.lapesd.freqel.query.parse;

import br.ufsc.lapesd.freqel.algebra.Op;

import javax.annotation.Nonnull;

public interface SPARQLParserService {
    @Nonnull Op parse(@Nonnull SPARQLParserOptions options,
                      @Nonnull String sparql) throws SPARQLParseException;
}
