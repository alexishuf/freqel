package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.std.StdLit;

import javax.annotation.Nonnull;

import static br.ufsc.lapesd.riefederator.model.term.std.StdLit.fromUnescaped;

public class EndpointTestBase implements TestContext {
    public static final @Nonnull StdLit A_AGE = fromUnescaped("23", xsdInt);
    public static final @Nonnull StdLit A_NAME = fromUnescaped("alice", "en");
    public static final @Nonnull StdLit B_NAME1 = fromUnescaped("bob", "en");
    public static final @Nonnull StdLit B_NAME2 = fromUnescaped("beto", "pt");

    public static class Fixture<T extends TPEndpoint> implements AutoCloseable {
        public final @Nonnull T endpoint;

        public Fixture(@Nonnull T endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void close() { }
    }
}
