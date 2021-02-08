package br.ufsc.lapesd.freqel.algebra.leaf;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;

public interface EndpointOp extends Op {
    @Nonnull TPEndpoint getEndpoint();
    default @Nonnull TPEndpoint getEffectiveEndpoint() {
        return getEndpoint().getEffective();
    }
}
