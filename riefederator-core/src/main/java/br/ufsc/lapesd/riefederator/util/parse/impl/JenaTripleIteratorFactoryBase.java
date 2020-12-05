package br.ufsc.lapesd.riefederator.util.parse.impl;

import br.ufsc.lapesd.riefederator.util.parse.JenaTripleIteratorFactory;
import br.ufsc.lapesd.riefederator.util.parse.RDFIterationDispatcher;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;

public abstract class JenaTripleIteratorFactoryBase implements JenaTripleIteratorFactory {
    protected RDFIterationDispatcher dispatcher = RDFIterationDispatcher.get();

    @Override public void attachTo(@Nonnull RDFIterationDispatcher dispatcher) {
        Preconditions.checkState(this.dispatcher == null || this.dispatcher == dispatcher);
        this.dispatcher = dispatcher;
    }
}
