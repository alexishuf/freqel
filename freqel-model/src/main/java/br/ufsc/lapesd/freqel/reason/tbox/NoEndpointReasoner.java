package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.algebra.leaf.EndpointOp;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Provider;
import java.util.Objects;
import java.util.function.Function;

public class NoEndpointReasoner implements EndpointReasoner {
    public static final NoEndpointReasoner INSTANCE = new NoEndpointReasoner();
    private @Nullable TBox tBox;

    public static class SingletonProvider implements Provider<NoEndpointReasoner> {
        @Override public @Nonnull NoEndpointReasoner get() { return INSTANCE; }
    }

    @Override public void offerTBox(@Nonnull TBox tBox) { this.tBox = tBox; }
    @Override public boolean acceptDisjunctive() { return false; }

    @Override public @Nonnull Results apply(@Nonnull EndpointOp op,
                                            @Nonnull Function<EndpointOp, Results> executor) {
        assert op.modifiers().reasoning() == null
                : "EndpointReasoner.apply() should not receive EndpointOps with reasoning modifier";
        return executor.apply(op);
    }

    @Override public String toString() {
        String tBoxString = Objects.toString(tBox);
        if (tBoxString.length() > 60)
            tBoxString = tBoxString.substring(0, 60);
        return String.format("%s(%s)", NoEndpointReasoner.class.getSimpleName(), tBoxString);
    }
}
