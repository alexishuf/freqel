package br.ufsc.lapesd.freqel.federation.execution.tree;

import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;

public interface OpExecutor {
    /**
     * Indicates whether this executor is able to handle nodes of the given class.
     *
     * @param nodeClass {@link Op} to evaluate
     * @return <code>true</code> iff execute() can be called for that node.
     */
    boolean canExecute(@Nonnull Class<? extends Op> nodeClass);

    /**
     * Returns a {@link Results} object with the results of execution of the node.
     *
     * Typically the returned {@link Results} object is lazy, meaning that only preliminary
     * work is done by this method and most of the processing/communication occurs as
     * {@link Results}.next() is called. The {@link Results} object also SHOULD own the necessary
     * resources, meaning that only the {@link Results} object needs to be closed to release
     * them and the {@link Op} executor MAY be collected before the {@link Results}
     * object is completely consumed.
     *
     * @param node node to execute
     * @return A new {@link Results} object
     * @throws IllegalArgumentException iff <code>canExecute(node.getClass()) == false</code>
     */
    @Nonnull Results execute(@Nonnull Op node) throws IllegalArgumentException;
}
