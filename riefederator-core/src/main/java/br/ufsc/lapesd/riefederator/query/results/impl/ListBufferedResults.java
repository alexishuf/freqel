package br.ufsc.lapesd.riefederator.query.results.impl;

import br.ufsc.lapesd.riefederator.query.results.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class ListBufferedResults extends DelegatingResults implements BufferedResults {
    private static final Logger logger = LoggerFactory.getLogger(ListBufferedResults.class);

    private final @Nonnull Results original;
    private final @Nonnull List<Solution> buffer;
    private boolean buffered;

    public static final Factory FACTORY = ListBufferedResults::new;

    public ListBufferedResults(@Nonnull Results in) {
        super(in.getVarNames(), in);
        this.original = in;
        this.buffer = new ArrayList<>();
    }

    public static @Nonnull BufferedResults applyIfNotBuffered(@Nonnull Results in) {
        return in instanceof BufferedResults ? (BufferedResults)in : new ListBufferedResults(in);
    }

    @Override
    public boolean isOrdered() {
        return true;
    }

    @Override
    public void reset(boolean close) {
        if (!buffered)
            logger.warn("Doing reset() on not fully buffered Results!");
        in = new CollectionResults(buffer, original.getVarNames());
        if (close)
            original.close();
    }

    @Override
    public boolean hasNext() {
        boolean has = in.hasNext();
        if (!has && !buffered && in == original)
            buffered = true;
        return has;
    }

    @Override
    public @Nonnull Solution next() {
        Solution solution = in.next();
        if (!buffered) {
            assert in == original;
            buffer.add(solution);
        }
        return solution;
    }

    @Override
    public void close() throws ResultsCloseException {
        original.close();
    }
}
