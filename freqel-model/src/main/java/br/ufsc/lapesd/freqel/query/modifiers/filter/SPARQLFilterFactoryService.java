package br.ufsc.lapesd.freqel.query.modifiers.filter;

import br.ufsc.lapesd.freqel.query.modifiers.FilterParsingException;

import javax.annotation.Nonnull;

public interface SPARQLFilterFactoryService {
    @Nonnull SPARQLFilter parse(@Nonnull String sparqlFilter) throws FilterParsingException;
    @Nonnull SPARQLFilter parse(@Nonnull SPARQLFilterNode root) throws FilterParsingException;
    @Nonnull SPARQLFilterExecutor createExecutor();
}
