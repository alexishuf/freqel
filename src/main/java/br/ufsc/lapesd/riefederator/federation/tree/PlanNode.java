package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public interface PlanNode {
    /**
     * Names of variables that will be results of this node.
     *
     * @return a subset of {@link PlanNode#getAllVars()}
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
     * @return union of {@link PlanNode#getResultVars()} and {@link PlanNode#getInputVars()}.
     */
    @Nonnull Set<String> getPublicVars();

    /**
     * Result variables that are not inputs.
     *
     * @return A subset of {@link PlanNode#getResultVars()} that does not intersect with
     *         {@link PlanNode#getRequiredInputVars()}.
     */
    @Nonnull Set<String> getStrictResultVars();

    /**
     * Variables that are either required or optional inputs.
     * @return The union of {@link PlanNode#getRequiredInputVars()}
     *         and {@link PlanNode#getOptionalInputVars()}.
     */
    @Nonnull Set<String> getInputVars();

    /**
     * Variables which must receive a value (e.g., bind) before the node is executable.
     * @return A subset of {@link PlanNode#getAllVars()}.
     */
    @Nonnull Set<String> getRequiredInputVars();

    /**
     * Variables that may act as either inputs or outputs.
     * @return A subset of {@link PlanNode#getAllVars}.
     */
    @Nonnull Set<String> getOptionalInputVars();

    /**
     * Get the set of matched triples. This is a subset of the original {@link CQuery}
     * that yielded this plan (even when the plan includes rewritten triples).
     */
    @Nonnull Set<Triple> getMatchedTriples();

    boolean hasInputs();

    boolean hasRequiredInputs();

    @Nonnull List<PlanNode> getChildren();

    boolean isProjecting();

    @Nonnull Cardinality getCardinality();

    /**
     * Add a filter to this node.
     *
     * @param filter filter to add
     * @return true if the filter was added, false if filter was already present
     */
    @CanIgnoreReturnValue
    boolean addFilter(@Nonnull SPARQLFilter filter);

    /**
     * Remove a filter from this node
     * @param filter filter to remove
     * @return true if the filter was removed (was present), false otherwise
     */
    @CanIgnoreReturnValue
    boolean removeFilter(@Nonnull SPARQLFilter filter);

    @Nonnull Set<SPARQLFilter> getFilers();

    <T extends TermAnnotation>
    boolean forEachTermAnnotation(@Nonnull Class<T> cls, @Nonnull BiConsumer<Term, T> consumer);

    /**
     * Return a new tree with the variables in solution bound to the respective {@link Term}s
     *
     * @param solution source of bindings
     * @return new plan tree
     */
    @Nonnull PlanNode createBound(@Nonnull Solution solution);

    /**
     * Creates a new instance replacing the children which are keys in map with
     * the corresponding value.
     *
     * @throws IllegalArgumentException if <code>!getChildren().contains(child)</code>
     * @return a new {@link PlanNode}
     */
    @Nonnull PlanNode replacingChildren(@Nonnull Map<PlanNode, PlanNode> map)
            throws IllegalArgumentException;

    @Contract("_ -> param1") @CanIgnoreReturnValue
    @Nonnull StringBuilder toString(@Nonnull StringBuilder builder);

    @Nonnull String prettyPrint();

    @Contract("_, _ -> param1") @CanIgnoreReturnValue
    @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder, @Nonnull String indent);
}
