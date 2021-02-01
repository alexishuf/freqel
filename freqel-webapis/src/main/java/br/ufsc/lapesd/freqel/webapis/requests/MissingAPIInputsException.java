package br.ufsc.lapesd.freqel.webapis.requests;

import br.ufsc.lapesd.freqel.webapis.requests.impl.APIRequestExecutorException;

import javax.annotation.Nonnull;
import java.util.Collection;

public class MissingAPIInputsException extends APIRequestExecutorException {
    private final @Nonnull Collection<String> missing;
    private final @Nonnull APIRequestExecutor executor;

    public MissingAPIInputsException(@Nonnull Collection<String> missing,
                                     @Nonnull APIRequestExecutor executor) {
        super("Executor " + executor + " is missing required arguments " + missing);
        this.missing = missing;
        this.executor = executor;
    }

    public MissingAPIInputsException(@Nonnull String details, @Nonnull Collection<String> missing,
                                     @Nonnull APIRequestExecutor executor) {
        super("Executor " + executor + " is missing required arguments " + missing + "." + details);
        this.missing = missing;
        this.executor = executor;
    }

    public @Nonnull Collection<String> getMissing() {
        return missing;
    }

    public @Nonnull APIRequestExecutor getExecutor() {
        return executor;
    }
}
