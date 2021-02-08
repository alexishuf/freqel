package br.ufsc.lapesd.freqel.description;

import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import java.util.function.Function;

public class EmptyDescription implements Description {
    public static final EmptyDescription INSTANCE = new EmptyDescription();
    public static final Function<TPEndpoint, Description> FACTORY = e -> INSTANCE;

    @Override public @Nonnull CQueryMatch match(@Nonnull CQuery query,
                                                @Nonnull MatchReasoning reasoning) {
        return CQueryMatch.EMPTY;
    }

    @Override public @Nonnull CQueryMatch localMatch(@Nonnull CQuery query,
                                                     @Nonnull MatchReasoning reasoning) {
        return CQueryMatch.EMPTY;
    }

    @Override public boolean supports(@Nonnull MatchReasoning mode) {
        return MatchReasoning.NONE.equals(mode);
    }

    @Override public void update() { }
    @Override public void   init() { }
    @Override public boolean waitForInit(int timeoutMilliseconds) { return true; }
    @Override public boolean  updateSync(int timeoutMilliseconds) { return true; }
}
