package br.ufsc.lapesd.freqel.query.endpoint;

import br.ufsc.lapesd.freqel.algebra.Op;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UnrelatedEndpointException extends DQEndpointException {
    private final @Nullable Op op;
    private final @Nullable DQEndpoint targetEndpoint;

    public UnrelatedEndpointException(@Nonnull String message,
                                      @Nullable Op op, @Nullable DQEndpoint targetEndpoint) {
        super(message);
        this.op = op;
        this.targetEndpoint = targetEndpoint;
    }

    public UnrelatedEndpointException(@Nonnull Op op, @Nullable DQEndpoint targetEndpoint) {
        this("Endpoint != "+targetEndpoint+" on query "+op, op, targetEndpoint);
    }

    public @Nullable Op getOp() {
        return op;
    }

    public @Nullable DQEndpoint getTargetEndpoint() {
        return targetEndpoint;
    }
}
