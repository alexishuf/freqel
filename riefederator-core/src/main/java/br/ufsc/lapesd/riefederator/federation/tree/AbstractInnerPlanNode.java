package br.ufsc.lapesd.riefederator.federation.tree;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public abstract class AbstractInnerPlanNode extends AbstractPlanNode {
    protected  @Nullable Set<String> allVarsCache;
    protected @Nullable Set<String> resultVarsCache, reqInputsCache, optInputsCache;
    protected boolean hasInputs;
    private @Nonnull ImmutableList<PlanNode> children;
    private @Nullable SoftReference<Set<Triple>> matchedTriples;

    private static boolean checkHasInputs(Collection<PlanNode> children,
                                          @Nullable Set<String> projection) {
        assert projection == null ||
                children.stream().flatMap(n -> n.getRequiredInputVars().stream())
                                 .allMatch(projection::contains);
        return children.stream().anyMatch(PlanNode::hasInputs);
    }

    public AbstractInnerPlanNode(@Nonnull Collection<PlanNode> children,
                                 @Nonnull Cardinality cardinality,
                                 @Nullable Set<String> projection, boolean hasInputs) {
        super(cardinality, projection);
        this.children = ImmutableList.copyOf(children);
        this.hasInputs = hasInputs;
        assert projection == null ||
                CollectionUtils.union(children, PlanNode::getResultVars).containsAll(projection)
                : "Projection contains variables that are not result in any child";
    }

    public AbstractInnerPlanNode(@Nonnull Collection<PlanNode> children,
                                 @Nonnull Cardinality cardinality,
                                 @Nullable Set<String> projection) {
        this(children, cardinality, projection, checkHasInputs(children, projection));
    }

    @Override
    public @Nonnull Set<String> getAllVars() {
        if (allVarsCache == null) {
            allVarsCache = children.stream().flatMap(n -> n.getPublicVars().stream())
                                   .collect(toSet());
        }
        return allVarsCache;
    }


    @Override
    public @Nonnull Set<String> getResultVars() {
        if (projection != null)
            return projection;
        if (resultVarsCache == null)
            resultVarsCache = CollectionUtils.union(children, PlanNode::getResultVars);
        return resultVarsCache;
    }

    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        if (reqInputsCache == null) {
            reqInputsCache = children.stream().flatMap(n -> n.getRequiredInputVars().stream())
                                     .collect(toSet());
            assert reqInputsCache.isEmpty() || hasInputs();
        }
        return reqInputsCache;
    }

    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        if (optInputsCache == null) {
            Set<String> required = getRequiredInputVars();
            optInputsCache = children.stream().flatMap(n -> n.getOptionalInputVars().stream())
                                      .filter(n -> !required.contains(n))
                                      .collect(toSet());
            assert optInputsCache.isEmpty() || hasInputs();
        }
        return optInputsCache;
    }

    @Override
    public boolean hasInputs() {
        return hasInputs;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        Set<Triple> strong = matchedTriples == null ? null : matchedTriples.get();
        if (strong == null) {
            strong = new HashSet<>();
            for (PlanNode child : getChildren())
                strong.addAll(child.getMatchedTriples());
            matchedTriples = new SoftReference<>(strong);
        }
        return strong;
    }

    @Override
    public @Nonnull List<PlanNode> getChildren() {
        return children;
    }
}
