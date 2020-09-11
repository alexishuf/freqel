package br.ufsc.lapesd.riefederator.query.results;

import javax.annotation.Nonnull;
import java.util.Set;

public abstract class DelegatingResults extends AbstractResults {
    protected @Nonnull Results in;

    protected DelegatingResults(@Nonnull Set<String> varNames, @Nonnull Results in) {
        super(varNames);
        this.in = in;
    }

    @Override
    public int getReadyCount() {
        return in.getReadyCount();
    }

    @Override
    public boolean isAsync() {
        return in.isAsync();
    }

    @Override
    public boolean isOptional() {
        return super.isOptional() || in.isOptional();
    }

    @Override
    public int getLimit() {
        return in.getLimit();
    }

    @Override
    public boolean isDistinct() {
        return in.isDistinct();
    }

    @Override
    public boolean hasNext() {
        return in.hasNext();
    }

    @Override
    public void close() throws ResultsCloseException {
        in.close();
    }
}
