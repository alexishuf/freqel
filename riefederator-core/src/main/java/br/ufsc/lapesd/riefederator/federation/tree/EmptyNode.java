package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.setMinus;
import static java.util.Collections.emptySet;

public class EmptyNode extends AbstractPlanNode {
    private @Nullable CQuery query;
    private Set<String> allVars, resultVars, reqInputs, optInputs;

    public EmptyNode(@Nonnull Collection<String> resultVars) {
        this(resultVars, emptySet(), emptySet());
    }

    public EmptyNode(@Nonnull Collection<String> allVars,
                     @Nonnull Collection<String> reqInputVars,
                     @Nonnull Collection<String> optInputVars) {
        this(allVars, allVars, reqInputVars, optInputVars);
    }

    public EmptyNode(@Nonnull Collection<String> allVars,
                     @Nonnull Collection<String> resultVars,
                     @Nonnull Collection<String> reqInputVars,
                     @Nonnull Collection<String> optInputVars) {
        super(Cardinality.EMPTY, null);
        this.allVars = ImmutableSet.copyOf(allVars);
        this.resultVars = ImmutableSet.copyOf(resultVars);
        this.reqInputs = ImmutableSet.copyOf(reqInputVars);
        this.optInputs = ImmutableSet.copyOf(optInputVars);
        assert resultVars.size() <= allVars.size();
        assert reqInputVars.size() <= allVars.size();
        assert optInputVars.size() <= allVars.size();
        assert allVars.containsAll(resultVars);
        assert allVars.containsAll(reqInputVars);
        assert allVars.containsAll(optInputVars);
        assertAllInvariants();
    }

    public EmptyNode(@Nonnull CQuery query) {
        this(query.attr().allVarNames());
        this.query = query;
    }

    @Override
    public @Nonnull Set<String> getAllVars() {
        return allVars;
    }

    @Override
    public @Nonnull Set<String> getResultVars() {
        return resultVars;
    }

    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        return reqInputs;
    }

    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        return optInputs;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        return query == null ? emptySet() : query.attr().matchedTriples();
    }

    @Override
    public @Nonnull
    AbstractPlanNode createBound(@Nonnull Solution solution) {
        Collection<String> names = solution.getVarNames();
        Set<String> resultVars = setMinus(getResultVars(), names);
        Set<String> reqInputs = setMinus(getRequiredInputVars(), names);
        Set<String> optInputs = setMinus(getOptionalInputVars(), names);
        EmptyNode bound = new EmptyNode(resultVars, reqInputs, optInputs);
        bound.addBoundFiltersFrom(getFilters(), solution);
        return bound;
    }

    @Override
    public @Nonnull EmptyNode
    replacingChildren(@Nonnull Map<PlanNode, PlanNode> map) throws IllegalArgumentException {
        return this;
    }

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        return builder.append("EMPTY").append(getVarNamesString());
    }

    @Override
    public @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder, @Nonnull String indent) {
        return builder.append(indent).append(toString()).append(' ').append(getName());
    }
}
