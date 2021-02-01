package br.ufsc.lapesd.freqel.jena.query.modifiers.filter;

import br.ufsc.lapesd.freqel.query.modifiers.FilterParsingException;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterExecutor;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterFactoryService;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterNode;

import javax.annotation.Nonnull;

public class JenaSPARQLFilterFactoryService implements SPARQLFilterFactoryService {
    @Override
    public @Nonnull SPARQLFilter parse(@Nonnull String sparqlFilter) throws FilterParsingException {
        return JenaSPARQLFilter.build(sparqlFilter);
    }

    @Override public @Nonnull SPARQLFilter
    parse(@Nonnull SPARQLFilterNode root) throws FilterParsingException {
        return JenaSPARQLFilter.build(root);
    }

    @Override public @Nonnull SPARQLFilterExecutor createExecutor() {
        return new JenaSPARQLFilterExecutor();
    }
}
