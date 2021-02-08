package br.ufsc.lapesd.freqel.query.endpoint.decorators;

import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class UncloseableTPEndpoint extends AbstractTPEndpointDecorator implements TPEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(UncloseableTPEndpoint.class);
    private final @Nonnull TPEndpoint delegate;

    public UncloseableTPEndpoint(@Nonnull TPEndpoint delegate) {
        super(delegate);
        this.delegate = delegate;
    }

    @Override public void close() {
        logger.debug("Ignoring {}.close()", this);
    }

    @Override public @Nonnull String toString() {
        return "uncloseable("+delegate.toString()+")";
    }
}
