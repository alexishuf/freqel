package br.ufsc.lapesd.riefederator.query.results;

import javax.annotation.Nonnull;

public interface BufferedResults extends Results {

    @FunctionalInterface
    interface Factory {
        /**
         * Crea te a new {@link BufferedResults} instance buffering solutions from <code>in</code>.
         */
        @Nonnull BufferedResults create(@Nonnull Results in);
    }

    /**
     * If true, iteration after {@link BufferedResults#reset()} will retain the order of the first.
     */
    boolean isOrdered();

    /**
     * Repositions the {@link Results} iteration at the beginning.
     *
     * Calling this before {@link Results#hasNext()} returned false will discard some
     * results of the input {@link Results} object.
     *
     * @param close if true, the input {@link Results} object will be closed.
     *
     * @throws ResultsCloseException thrown by {@link Results#close()} on the input
     *                               {@link Results} object. Will never be thrown if
     *                               <code>!close</code>
     */
    void reset(boolean close) throws ResultsCloseException ;

    default void reset() {reset(true);}
}
