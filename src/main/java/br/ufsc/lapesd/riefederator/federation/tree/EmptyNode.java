package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Solution;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.setMinus;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public class EmptyNode extends PlanNode {
    private @Nullable CQuery query;

    public EmptyNode(@Nonnull Collection<String> resultVars) {
        this(resultVars, emptySet());
    }

    public EmptyNode(@Nonnull Collection<String> resultVars,
                     @Nonnull Collection<String> inputVars) {
        super(resultVars, false, inputVars, emptyList());
    }

    public EmptyNode(@Nonnull CQuery query) {
        this(query.streamTerms(Var.class).map(Var::getName).collect(toSet()));
        this.query = query;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        Set<Triple> strong = matchedTriples.get();
        if (strong == null) {
            strong = query != null ? query.getMatchedTriples() : emptySet();
            matchedTriples = new SoftReference<>(strong);
        }
        return strong;
    }

    @Override
    public @Nonnull PlanNode createBound(@Nonnull Solution solution) {
        Collection<String> names = solution.getVarNames();
        return new EmptyNode(setMinus(getResultVars(), names), setMinus(getInputVars(), names));
    }

    @Override
    public @Nonnull EmptyNode replacingChildren(@Nonnull Map<PlanNode, PlanNode> map)
            throws IllegalArgumentException {
        Preconditions.checkArgument(map.isEmpty());
        return this;
    }

    @Override
    protected @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        return builder.append("EMPTY").append(getVarNamesString());
    }

    @Override
    protected @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder, @Nonnull String indent) {
        return builder.append(indent).append(toString());
    }
}
