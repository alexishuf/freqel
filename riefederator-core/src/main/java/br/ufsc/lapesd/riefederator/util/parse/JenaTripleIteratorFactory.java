package br.ufsc.lapesd.riefederator.util.parse;

import br.ufsc.lapesd.riefederator.util.parse.iterators.JenaTripleIterator;

import javax.annotation.Nonnull;

public interface JenaTripleIteratorFactory  {
    @Nonnull JenaTripleIterator create(@Nonnull Object source) throws SourceIterationException;
    boolean canCreate(@Nonnull Object source);
    void attachTo(@Nonnull RDFIterationDispatcher dispatcher);
}
