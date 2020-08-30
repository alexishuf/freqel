package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Limit;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.results.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class LimitResults extends DelegatingResults implements BufferedResults {
    private static final Logger logger = LoggerFactory.getLogger(LimitResults.class);

    private final @Nonnull List<Solution> buffer = new ArrayList<>();
    private final int limit;
    private @Nonnull final Results original;
    private @Nullable Solution next;
    private boolean buffering = true;

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        return applyIf(in, query.getModifiers());
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull ModifiersSet modifiers) {
        Limit limit = modifiers.limit();
        int value = limit == null ? 0 : limit.getValue();
        if (value > 0)
            return new LimitResults(in, value);
        return in;
    }

    public LimitResults(@Nonnull Results in, int limit) {
        super(in.getVarNames(), in);
        this.limit = limit;
        this.original = in;
    }

    @Override
    public boolean isOrdered() {
        return true;
    }

    @Override
    public void reset(boolean close) throws ResultsCloseException {
        if (buffering && hasNext())
            logger.warn("Resetting a LimitResults with available inputs and before the limit");
        in = new CollectionResults(buffer, getVarNames());
        if (buffering && close)
            original.close();
        buffering = false;
    }

    @Override
    public boolean hasNext() {
        if (next != null)
            return true;
        if (!buffering) {
            if (in.hasNext())
                next = in.next();
        } else {
            if (buffer.size() < limit && in.hasNext())
                buffer.add(next = in.next());
        }
        return next != null;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        Solution solution = this.next;
        assert solution != null;
        this.next = null;
        return solution;
    }

    @Override
    public void close() throws ResultsCloseException {
        ResultsCloseException exception = null;
        try {
            super.close();
        } catch (ResultsCloseException e) {
            exception = e;
        }
        try {
            original.close();
        } catch (ResultsCloseException e) {
            if (exception != null) exception.addSuppressed(e);
            else exception = e;
        }
        if (exception != null)
            throw exception;
    }
}
