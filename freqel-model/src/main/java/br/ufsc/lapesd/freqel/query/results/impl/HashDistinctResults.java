package br.ufsc.lapesd.freqel.query.results.impl;

import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.freqel.query.results.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.NoSuchElementException;

public class HashDistinctResults extends DelegatingResults implements BufferedResults {
    private static final Logger logger = LoggerFactory.getLogger(HashDistinctResults.class);

    private final @Nonnull HashSet<Solution> set = new HashSet<>();
    private final @Nonnull Results original;
    private Solution next = null;
    private boolean wasReset = false;

    public static final @Nonnull Factory FACTORY = HashDistinctResults::new;

    public HashDistinctResults(@Nonnull Results input) {
        super(input.getVarNames(), input);
        this.original = input;
    }

    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull ModifiersSet modifiers) {
        if (modifiers.distinct() != null && !in.isDistinct())
            return new HashDistinctResults(in);
        return in;
    }
    public static @Nonnull Results applyIf(@Nonnull Results in, @Nonnull CQuery query) {
        return applyIf(in, query.getModifiers());
    }

    public static @Nonnull Results applyIfNotDistinct(@Nonnull Results in) {
        return in.isDistinct() ? in : new HashDistinctResults(in);
    }

    @Override
    public boolean isOrdered() {
        return false;
    }

    @Override
    public boolean isDistinct() {
        return true;
    }

    @Override
    public void reset(boolean close) throws ResultsCloseException {
        if (!wasReset && original.hasNext())
            logger.warn("Input iterator {} still has results, reset() will discard them", original);
        in = new CollectionResults(set, original.getVarNames());
        if (close && !wasReset)
            original.close();
        wasReset = true;
    }

    @Override
    public int getReadyCount() {
        return in.getReadyCount() + (next != null ? 1 : 0);
    }

    @Override
    public boolean hasNext() {
        while (this.next == null && in.hasNext()) {
            Solution next = in.next();
            if (wasReset || set.add(next))
                this.next = next;
        }
        return this.next != null;
    }

    @Override
    public @Nonnull Solution next() {
        if (!hasNext())
            throw new NoSuchElementException();
        Solution current = this.next;
        this.next = null;
        return current;
    }

    @Override
    public void close() throws ResultsCloseException {
        super.close();
        original.close();
    }
}
