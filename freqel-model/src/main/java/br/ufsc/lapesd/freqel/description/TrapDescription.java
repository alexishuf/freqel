package br.ufsc.lapesd.freqel.description;

import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import java.util.function.Function;

public class TrapDescription implements Description {
    public static final TrapDescription INSTANCE = new TrapDescription();
    public static final Function<TPEndpoint, Description> FACTORY = e -> INSTANCE;

    @Override public @Nonnull CQueryMatch match(@Nonnull CQuery query,
                                                @Nonnull MatchReasoning reasoning) {
        assert false : "This Description instance should have not been called";
        return CQueryMatch.EMPTY;
    }

    @Override public @Nonnull CQueryMatch localMatch(@Nonnull CQuery query,
                                                      @Nonnull MatchReasoning reasoning) {
        assert false : "This Description instance should have not been called";
        return CQueryMatch.builder(query).allUnknown().build();
    }

    @Override public boolean supports(@Nonnull MatchReasoning mode) {
        return MatchReasoning.NONE.equals(mode);
    }

    @Override public void update() {
        assert false : "This Description instance should have not been called";
    }

    @Override public void init() {
        assert false : "This Description instance should have not been called";
    }

    @Override public boolean waitForInit(int timeoutMilliseconds) {
        assert false : "This Description instance should have not been called";
        return true;
    }

    @Override public boolean updateSync(int timeoutMilliseconds) {
        assert false : "This Description instance should have not been called";
        return true;
    }
}
