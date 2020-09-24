package br.ufsc.lapesd.riefederator.algebra.inner;

import br.ufsc.lapesd.riefederator.algebra.AbstractOp;
import br.ufsc.lapesd.riefederator.algebra.InnerOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.TakenChildren;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import br.ufsc.lapesd.riefederator.util.ref.IdentityHashSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.union;
import static com.google.common.base.Preconditions.*;
import static java.util.Collections.emptySet;

public abstract class AbstractInnerOp extends AbstractOp implements InnerOp {
    protected @Nullable Set<String> allVarsCache, resultVarsCache, reqInputsCache, optInputsCache;
    protected @Nullable IndexSet<Triple> triplesUniverse;
    protected @Nullable IndexSet<String> varsUniverse;
    private  @Nullable List<Op> children;
    private @Nullable Set<Triple> matchedTriples;
    private @Nonnull final ModifiersSet modifiers = new ModifiersSet();

    protected AbstractInnerOp(@Nonnull Collection<Op> children) {
        this.children = children instanceof List ? (List<Op>)children
                                                 : new ArrayList<>(children);
        for (Op child : this.children) {
            child.attachTo(this);
            if (triplesUniverse == null) {
                IndexSet<Triple> set = child.getOfferedTriplesUniverse();
                if (set != null) triplesUniverse = set;
            }
            if (varsUniverse == null) {
                IndexSet<String> set = child.getOfferedVarsUniverse();
                if (set != null) varsUniverse = set;
            }
        }
        assert children.stream().map(Op::getOfferedTriplesUniverse).distinct().count() <= 1;
        assert children.stream().map(Op::getOfferedVarsUniverse).distinct().count() <= 1;
    }

    @Override public void offerTriplesUniverse(@Nonnull IndexSet<Triple> universe) {
        if (triplesUniverse != universe) {
            triplesUniverse = universe;
            matchedTriples = null;
            //noinspection AssertWithSideEffects
            assert assertAllInvariants(true);
        }
    }

    @Override public @Nullable IndexSet<Triple> getOfferedTriplesUniverse() {
        return triplesUniverse;
    }

    @Override public void offerVarsUniverse(@Nonnull IndexSet<String> universe) {
        if (varsUniverse != universe) {
            varsUniverse = universe;
            strictResultVarsCache = publicVarsCache = allInputVarsCache = null;
            allVarsCache = resultVarsCache = reqInputsCache = optInputsCache = null;
            //noinspection AssertWithSideEffects
            assert assertAllInvariants(true);
        }
    }

    @Override public @Nullable IndexSet<String> getOfferedVarsUniverse() {
        return varsUniverse;
    }

    @Override
    protected boolean assertAllInvariants(boolean test) {
        if (!test || !AbstractInnerOp.class.desiredAssertionStatus())
            return true;
        Set<String> oldAllVarsCache = allVarsCache;
        Set<String> oldResultVarsCache = resultVarsCache;
        Set<String> oldReqInputsCache = reqInputsCache;
        Set<String> oldOptInputsCache = optInputsCache;
        try {
            assert children == null || new IdentityHashSet<>(children).size() == children.size()
                    : "Duplicate children (same instance appears twice)";
            assert children == null || new HashSet<>(children).size() == children.size()
                    : "Duplicate children (there are equal, but distinct, instances)";
            return super.assertAllInvariants(true);
        } finally {
            allVarsCache = oldAllVarsCache;
            resultVarsCache = oldResultVarsCache;
            reqInputsCache = oldReqInputsCache;
            optInputsCache = oldOptInputsCache;
        }
    }

    @Override
    public void purgeCachesShallow() {
        super.purgeCachesShallow();
        allVarsCache = resultVarsCache = reqInputsCache = optInputsCache = null;
        matchedTriples = null;
    }

    @Override
    public @Nonnull ModifiersSet modifiers() {
        return modifiers;
    }

    @Override
    public @Nonnull Set<String> getAllVars() {
        if (allVarsCache == null) {
            cacheHit = true;
            assert children != null;
            if (varsUniverse != null)
                allVarsCache = union(varsUniverse, children, Op::getPublicVars);
            else
                allVarsCache = union(children, Op::getPublicVars, 16);
        }
        return allVarsCache;
    }

    @Override
    public @Nonnull Set<String> getResultVars() {
        if (resultVarsCache == null) {
            cacheHit = true;
            assert children != null;
            Projection projection = modifiers().projection();
            if (projection != null) {
                if (varsUniverse != null)
                    resultVarsCache = varsUniverse.subset(projection.getVarNames());
                else
                    resultVarsCache = projection.getVarNames();
            } else {
                if (varsUniverse != null)
                    resultVarsCache = union(varsUniverse, children, Op::getResultVars);
                else
                    resultVarsCache = union(children, Op::getResultVars, 16);
            }
        }
        return resultVarsCache;
    }

    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        if (reqInputsCache == null) {
            cacheHit = true;
            assert children != null;
            if (varsUniverse != null)
                reqInputsCache = union(varsUniverse, children, Op::getRequiredInputVars);
            else
                reqInputsCache = union(children, Op::getRequiredInputVars);
            assert reqInputsCache.isEmpty() || hasInputs();
        }
        return reqInputsCache;
    }

    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        if (optInputsCache == null) {
            cacheHit = true;
            assert children != null;
            Set<String> required = getRequiredInputVars();
            if (varsUniverse != null) {
                optInputsCache = union(varsUniverse, children, Op::getOptionalInputVars);
                optInputsCache.removeAll(required); //fast since required is a subset of universe
            } else {
                Set<String> set = null;
                for (Op child : children) {
                    Set<String> candidates = child.getOptionalInputVars();
                    for (String var : candidates) {
                        if (!required.contains(var)) {
                            if (set == null)
                                set = new HashSet<>(candidates.size() + 10);
                            set.add(var);
                        }
                    }
                }
                optInputsCache = set == null ? emptySet() : set;
            }
            assert optInputsCache.isEmpty() || hasInputs();
        }
        return optInputsCache;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        if (matchedTriples == null) {
            cacheHit = true;
            assert children != null;
            if (triplesUniverse != null)
                matchedTriples = union(triplesUniverse, children, Op::getMatchedTriples);
            else
                matchedTriples = union(children, Op::getMatchedTriples, 16);
        }
        return matchedTriples;
    }

    @Override
    public @Nullable Set<Triple> getCachedMatchedTriples() {
        return matchedTriples;
    }

    @Override
    public @Nonnull List<Op> getChildren() {
        checkState(children != null, "getChildren() before closing takeChildren() handle");
        return children;
    }

    @Override
    public @Nonnull Op setChild(int index, @Nonnull Op replacement) {
        assert children != null;
        checkPositionIndex(index, children.size());
        Op old = children.set(index, replacement);
        if (old == replacement) //compare by ==
            return old; // no effect
        old.detachFrom(this);
        replacement.attachTo(this);
        return old;
    }

    @Override
    public @Nonnull TakenChildren takeChildren() {
        checkState(children != null, "previous takeChildren() not closed");
        List<Op> old = this.children;
        detachChildren();
        return new TakenChildren(this, old);
    }

    @Override
    public @Nonnull List<Op> setChildren(@Nonnull List<Op> children) {
        List<Op> old = this.children == null ? Collections.emptyList() : this.children;
        this.children = children;
        for (Op child : children)
            child.attachTo(this);
        //noinspection AssertWithSideEffects
        assert assertAllInvariants(true);
        return old;
    }

    @Override
    public void detachChildren() {
        if (children != null) {
            for (Op child : children)
                child.detachFrom(this);
            children = null;
        }
    }

    @Override
    public void addChild(@Nonnull Op child) {
        checkState(children != null, "Previous takeChildren() handle not closed");
        checkArgument(!children.contains(child), "addChild(child): child is already a child");
        children.add(child);
        child.attachTo(this);
    }

    @Override
    public @Nonnull Op createBound(@Nonnull Solution solution) {
        List<Op> list = new ArrayList<>(getChildren().size());
        for (Op child : getChildren())
            list.add(child.createBound(solution));
        Op bound = createWith(list, null);
        TreeUtils.addBoundModifiers(bound.modifiers(), modifiers(), solution);
        return bound;
    }

    @Override
    public @Nonnull Op flatCopy() {
        checkState(children != null, "Previous takeChildren() handle not closed");
        Op copy = createWith(new ArrayList<>(getChildren()), modifiers());
        ((AbstractInnerOp)copy).copyCaches(this);
        copyCaches((AbstractInnerOp) copy);
        return copy;
    }

    @Override protected void copyCaches(@Nonnull AbstractOp other) {
        super.copyCaches(other);
        AbstractInnerOp otherInner = (AbstractInnerOp) other;
        varsUniverse    = otherInner.varsUniverse;
        allVarsCache    = otherInner.allVarsCache;
        resultVarsCache = otherInner.resultVarsCache;
        reqInputsCache  = otherInner.reqInputsCache;
        optInputsCache  = otherInner.optInputsCache;
        triplesUniverse = otherInner.triplesUniverse;
        matchedTriples  = otherInner.matchedTriples;
    }

    protected abstract @Nonnull StringBuilder prettyPrintNodeType(@Nonnull StringBuilder builder);

    protected abstract @Nonnull String toStringSeparator();

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder b) {
        checkState(children != null, "Previous takeChildren() handle not closed");
        if (isProjected())
            b.append(getPiWithNames()).append('(');
        String sep = toStringSeparator();
        for (Op child : getChildren())
            child.toString(b).append(sep);
        b.setLength(b.length()-sep.length());
        if (isProjected())
            b.append(')');
        return b;
    }

    @Override
    public @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                              @Nonnull String indent) {
        checkState(children != null, "Previous takeChildren() handle not closed");
        String indent2 = indent + "  ";
        builder.append(indent);
        if (isProjected())
            builder.append(getPiWithNames()).append('(');
        prettyPrintNodeType(builder);
        builder.append(isProjected() ? ")" : getVarNamesString())
                .append(' ').append(getCardinality()).append(' ').append(getName());
        boolean hasBreak = false;
        for (Modifier modifier : modifiers()) {
            if (modifier instanceof Projection) continue;
            if (!hasBreak) {
                builder.append('\n');
                hasBreak = true;
            }
            builder.append(indent2).append(modifier.toString().replace("\n", "\n"+indent2))
                   .append('\n');
        }
        if (getChildren().isEmpty()) {
            if (hasBreak)
                builder.setLength(builder.length()-1);
            return builder;
        }
        if (!hasBreak)
            builder.append('\n');
        for (Op child : getChildren())
            child.prettyPrint(builder, indent2).append('\n');
        builder.setLength(builder.length()-1);
        return builder;
    }
}
