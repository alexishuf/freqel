package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.Solution;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public interface PlanNode {
    @Nonnull Set<String> getResultVars();

    @Nonnull Set<String> getInputVars();

    @Nonnull Set<Triple> getMatchedTriples();

    boolean hasInputs();

    @Nonnull List<PlanNode> getChildren();

    boolean isProjecting();

    @Nonnull Cardinality getCardinality();

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
