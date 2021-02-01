package br.ufsc.lapesd.freqel.query.results.impl;

import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.modifiers.Limit;
import br.ufsc.lapesd.freqel.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.freqel.query.results.DelegatingResults;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsCloseException;
import br.ufsc.lapesd.freqel.query.results.Solution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.NoSuchElementException;

public class LimitResults extends DelegatingResults {
    private static final Logger logger = LoggerFactory.getLogger(LimitResults.class);
    private int consumed = 0;
    private final int limit;
    private ResultsCloseException closeException;

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        return applyIf(in, query.getModifiers());
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull ModifiersSet modifiers) {
        Limit limit = modifiers.limit();
        if (limit == null)
            return in;
        int value = limit.getValue(), current = in.getLimit();
        return current < 0 || current > value ?  new LimitResults(in, value) : in;
    }

    public LimitResults(@Nonnull Results in, int limit) {
        super(in.getVarNames(), in);
        this.limit = limit;
    }

    @Override
    public boolean hasNext() {
        return consumed < limit && in.hasNext();
    }

    @Override
    public boolean hasNext(int millisecondsTimeout) {
        return consumed < limit && in.hasNext(millisecondsTimeout);
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext()) throw new NoSuchElementException();
        ++consumed;
        Solution solution = in.next();
        if (consumed >= limit) {
            try {
                in.close();
            } catch (ResultsCloseException e) {
                logger.warn("{}: Failed to close input {}", this, in, e);
                closeException = e;
            }
        }
        return solution;
    }

    @Override
    public void close() throws ResultsCloseException {
        if (closeException != null) {
            throw new ResultsCloseException(this, "Failed to close input Results after " +
                                            "reaching limit", closeException);
        }
        in.close();
    }
}
