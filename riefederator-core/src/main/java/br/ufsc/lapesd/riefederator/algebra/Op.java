package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

public interface Op {
    /**
     * Get the name for this node within its plan.
     */
    @Nonnull String getName();

    /**
     * Set the name for this node within its plan.
     */
    void setName(@Nonnull String name);

    /**
     * Names of variables that will be results of this node.
     *
     * @return a subset of {@link Op#getAllVars()}
     */
    @Nonnull Set<String> getResultVars();

    /**
     * All variables in this, no be them inputs, results, or internal (projected-out) variables.
     */
    @Nonnull Set<String> getAllVars();

    /**
     * All variables that are either inputs or results.
     * Variables used only within filters never appear as result variables, but may appear
     * as inputs and here.
     *
     * @return union of {@link Op#getResultVars()} and {@link Op#getInputVars()}.
     */
    @Nonnull Set<String> getPublicVars();

    /**
     * Result variables that are not inputs.
     *
     * @return A subset of {@link Op#getResultVars()} that does not intersect with
     *         {@link Op#getRequiredInputVars()}.
     */
    @Nonnull Set<String> getStrictResultVars();

    /**
     * Variables that are either required or optional inputs.
     * @return The union of {@link Op#getRequiredInputVars()}
     *         and {@link Op#getOptionalInputVars()}.
     */
    @Nonnull Set<String> getInputVars();

    /**
     * Variables which must receive a value (e.g., bind) before the node is executable.
     * @return A subset of {@link Op#getAllVars()}.
     */
    @Nonnull Set<String> getRequiredInputVars();

    /**
     * Variables that may act as either inputs or outputs.
     * @return A subset of {@link Op#getAllVars}.
     */
    @Nonnull Set<String> getOptionalInputVars();

    boolean hasInputs();

    boolean hasRequiredInputs();

    /**
     * Indicates whether there is a Projection modifier on this {@link Op}.
     */
    boolean isProjected();

    /**
     * Get the set of matched triples. This is a subset of the original {@link CQuery}
     * that yielded this plan (even when the plan includes rewritten triples).
     */
    @Nonnull Set<Triple> getMatchedTriples();

    /**
     * Returns an unmodifiable view of the list of children nodes.
     */
    @Nonnull List<Op> getChildren();

    /**
     * Replaces the child at the given index with the new given Op.
     *
     * @param index index of the child to replace in {@link Op#getChildren()}
     * @param replacement new child node
     * @return the old child node
     */
    @Nonnull Op setChild(int index, @Nonnull Op replacement);

    /**
     * Send change notifications to the given listener when they occur.
     *
     * This is an idempotent operation: multiple attachments will not cause multiple
     * notifications. The order of notification has no relation to the order of attachment.
     */
    void attachListener(@Nonnull OpChangeListener listener);

    /**
     * Stop sending notifications to the given listener, if it was previously attached.
     */
    void detachListener(@Nonnull OpChangeListener listener);

    @Nonnull Cardinality getCardinality();

    /**
     * Changes the cardinality of this {@link Op} and returns the previous cardinality.
     */
    @CanIgnoreReturnValue
    @Nonnull Cardinality setCardinality(@Nonnull Cardinality cardinality);

    @Nonnull ModifiersSet modifiers();

    /**
     * Return a new tree with the variables in solution bound to the respective {@link Term}s
     *
     * @param solution source of bindings
     * @return new plan tree
     */
    @Nonnull Op createBound(@Nonnull Solution solution);

    @Contract("_ -> param1") @CanIgnoreReturnValue
    @Nonnull StringBuilder toString(@Nonnull StringBuilder builder);

    @Nonnull String prettyPrint();

    @Contract("_, _ -> param1") @CanIgnoreReturnValue
    @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder, @Nonnull String indent);
}
