package br.ufsc.lapesd.freqel.query.endpoint.exceptions;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.query.endpoint.DQEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UnsupportedOpException extends DQEndpointException {
    private @Nullable final Op op;
    private final @Nullable DQEndpoint endpoint;

    public UnsupportedOpException(String message, @Nullable Op op, @Nullable DQEndpoint endpoint) {
        super(message);
        this.op = op;
        this.endpoint = endpoint;
    }

    public UnsupportedOpException(@Nonnull Op op, @Nullable DQEndpoint endpoint) {
        this(op.getClass().getSimpleName()+" is not supported", op, endpoint);
    }

    public @Nullable Op getOp() {
        return op;
    }

    public @Nullable DQEndpoint getEndpoint() {
        return endpoint;
    }
}
