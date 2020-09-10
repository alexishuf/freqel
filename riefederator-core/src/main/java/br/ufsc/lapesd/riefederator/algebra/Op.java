package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface Op {
    /**
     * Recursively assert node invariants over the internal (and external) state.
     *
     * Computation is only performed if assertions are enabled. If assertions are not enabled,
     * this simply returns true without checking anything.
     */
    boolean assertTreeInvariants();

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
     * Same as {@link Op#getMatchedTriples()}, but will only return the set if it is
     * already computed or if its computation is fast.
     *
     * @return the set of triples if it was cached or fast to compute, false otherwise.
     */
    @Nullable Set<Triple> getCachedMatchedTriples();

    /**
     * Returns an unmodifiable view of the list of children nodes.
     */
    @Nonnull List<Op> getChildren();

    /**
     * A distinct collection of parent {@link Op} instances registered with {@link Op#attachTo(Op)}.
     */
    @Nonnull Collection<Op> getParents();

    /**
     * Record the given parent so it is notified by {@link Op#purgeCaches()}.
     */
    void attachTo(@Nonnull Op parent);

    /**
     * De-register parent as a parent. Further calls to {@link Op#purgeCaches()} will not notify it.
     */
    void detachFrom(@Nonnull Op parent);

    /**
     * Invalidate all caches held internally by this node and any of its direct or indirect parents.
     */
    void purgeCachesUpward();

    /**
     * Invalidate all caches held internally by this node and any of its direct or indirect
     * parents or children.
     */
    void purgeCaches();

    /**
     * Version of {@link Op#purgeCaches()} that does not propagate to parents nor children.
     */
    void purgeCachesShallow();

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

    /**
     * Creates a copy of this node that points to the same children instances and has
     * the same modifiers.
     *
     * Although the copy is identical, replacing its children or changing its modifiers will
     * not affect this instance.
     */
    @Nonnull Op flatCopy();

    @Contract("_ -> param1") @CanIgnoreReturnValue
    @Nonnull StringBuilder toString(@Nonnull StringBuilder builder);

    @Nonnull String prettyPrint();

    @Contract("_, _ -> param1") @CanIgnoreReturnValue
    @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder, @Nonnull String indent);
}
