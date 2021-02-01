package br.ufsc.lapesd.freqel.algebra;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.modifiers.Projection;
import br.ufsc.lapesd.freqel.util.CollectionUtils;
import br.ufsc.lapesd.freqel.util.ref.IdentityHashSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.*;

import static br.ufsc.lapesd.freqel.util.CollectionUtils.union;
import static java.util.Objects.requireNonNull;

public abstract class AbstractOp implements Op {
    protected @Nullable Set<String> strictResultVarsCache, publicVarsCache, allInputVarsCache;
    protected @Nonnull String name;
    private @Nonnull Cardinality cardinality = Cardinality.UNSUPPORTED;
    private @Nullable List<Op> parents = null;
    private boolean parentsSingleton = false;
    protected boolean cacheHit = false;

    protected AbstractOp() {
        this.name = "n-"+Integer.toHexString(System.identityHashCode(this));
    }

    @Override
    public @Nonnull String getName() {
        return name;
    }

    @Override
    public void setName(@Nonnull String name) {
        this.name = name;
    }

    @Override
    public boolean assertTreeInvariants() {
        if (!AbstractOp.class.desiredAssertionStatus())
            return true;
        boolean ok = assertAllInvariants(true);
        for (Op child : getChildren())
            ok &= child.assertTreeInvariants();
        return ok;
    }

    protected void assertAllInvariants() {
        assertAllInvariants(true);
    }

    protected boolean assertAllInvariants(boolean test) {
        if (!test || !AbstractOp.class.desiredAssertionStatus())
            return true;
        Set<String> oldStrictResultVarsCache = strictResultVarsCache;
        Set<String> oldPublicVarsCache = publicVarsCache;
        Set<String> oldAllInputVarsCache = allInputVarsCache;
        boolean oldCacheHit = this.cacheHit;
        try {
            Projection projection = modifiers().projection();
            assert projection == null || projection.getVarNames().equals(getResultVars());
            assert getPublicVars().containsAll(getResultVars());
            assert getPublicVars().containsAll(getInputVars());
            assert getInputVars().containsAll(getRequiredInputVars());
            assert getInputVars().containsAll(getOptionalInputVars());
            assert getResultVars().containsAll(getStrictResultVars());
            assert getStrictResultVars().stream().noneMatch(getInputVars()::contains);
            assert parents == null || new IdentityHashSet<>(parents).size() == parents.size()
                    : "Duplicate parents";
            return true;
        } finally {
            strictResultVarsCache = oldStrictResultVarsCache;
            publicVarsCache = oldPublicVarsCache;
            allInputVarsCache = oldAllInputVarsCache;
            cacheHit = oldCacheHit;
        }
    }

    @Override
    public @Nonnull Set<String> getStrictResultVars() {
        if (strictResultVarsCache == null) {
            cacheHit = true;
            if (hasInputs()) {
                strictResultVarsCache = CollectionUtils.setMinus(getResultVars(), getInputVars());
            } else {
                strictResultVarsCache = getResultVars();
            }
            assert modifiers().projection() == null
                    || requireNonNull(modifiers().projection())
                            .getVarNames().containsAll(strictResultVarsCache);
        }
        return strictResultVarsCache;
    }

    @Override
    public @Nonnull Set<String> getPublicVars() {
        if (publicVarsCache == null) {
            cacheHit = true;
            publicVarsCache = union(getResultVars(), getInputVars());
        }
        return publicVarsCache;
    }

    @Override
    public @Nonnull Set<String> getInputVars() {
        if (allInputVarsCache == null) {
            cacheHit = true;
            allInputVarsCache = union(getRequiredInputVars(), getOptionalInputVars());
        }
        return allInputVarsCache;
    }

    @Override
    public boolean hasInputs() {
        return !getOptionalInputVars().isEmpty() || !getRequiredInputVars().isEmpty();
    }

    @Override
    public boolean hasRequiredInputs() {
        return !getRequiredInputVars().isEmpty();
    }

    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        return Collections.emptySet();
    }

    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        return Collections.emptySet();
    }

    @Override
    public @Nonnull List<Op> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull List<Op> getParents() {
        return parents == null ? Collections.emptyList() : parents;
    }

    @Override
    public void attachTo(@Nonnull Op parent) {
        if (parent == this)
            throw new IllegalArgumentException("Node cannot be its own parent");
        assert parents == null || parents.stream().noneMatch(o -> o == parent)
                : "already attached to this parent";
        if (parents == null) {
            parentsSingleton = true;
            parents = Collections.singletonList(parent);
        } else if (parentsSingleton) {
            List<Op> list = new ArrayList<>();
            list.add(parents.get(0));
            list.add(parent);
            parentsSingleton = false;
            parents = list;
        } else {
            parents.add(parent);
        }
    }

    @Override
    public void detachFrom(@Nonnull Op parent) {
        boolean found = false;
        if (parents == null) {
            assert false : "detaching from parent, but has no parents";
        } else if (parentsSingleton) {
            if ((found = this.parents.get(0) == parent))
                this.parents = null;
            assert found : "detaching parent that is not a parent";
        } else {
            for (Iterator<Op> it = this.parents.iterator(); it.hasNext(); ) {
                if (it.next() == parent) {
                    found = true;
                    it.remove();
                    break;
                }
            }
            assert parents.stream().noneMatch(o -> o == parent) : "parent appears twice in list";
        }
        assert found : "parent does not appear in parents list";
    }

    @OverridingMethodsMustInvokeSuper protected void copyCaches(@Nonnull AbstractOp other) {
        strictResultVarsCache = other.strictResultVarsCache;
        publicVarsCache = other.publicVarsCache;
        allInputVarsCache = other.allInputVarsCache;
    }

    @Override
    public void purgeCachesShallow() {
        strictResultVarsCache = publicVarsCache = allInputVarsCache = null;
        cacheHit = false;
    }

    @Override
    public void purgeCachesUpward() {
        if (!cacheHit)
            return;
        ArrayDeque<Op> stack = new ArrayDeque<>();
        stack.push(this);
        purgeCachesUpward(stack);
    }

    public void purgeCachesUpward(@Nonnull ArrayDeque<Op> stack) {
        while (!stack.isEmpty()) {
            Op op = stack.pop();
            op.purgeCachesShallow();
            op.getParents().forEach(stack::push);
        }
    }

    @Override
    public void purgeCaches() {
        if (!cacheHit)
            return; // do not propagate if nobody queried this node since last cache purge
        ArrayDeque<Op> stack = new ArrayDeque<>();
        stack.push(this);
        while (!stack.isEmpty()) {
            Op op = stack.pop();
            op.purgeCachesShallow();
            op.getChildren().forEach(stack::push);
        }
        getParents().forEach(stack::push);
        purgeCachesUpward(stack);
    }

    @Override
    public @Nonnull Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public @Nonnull Cardinality setCardinality(@Nonnull Cardinality cardinality) {
        Cardinality old = this.cardinality;
        this.cardinality = cardinality;
        return old;
    }

    private @Nonnull String getVarNamesStringContent() {
        Set<String> results = getResultVars(), inputs = getRequiredInputVars();
        if (results.isEmpty() && inputs.isEmpty()) return "";
        StringBuilder builder = new StringBuilder();
        for (String out : results) {
            if (inputs.contains(out))
                builder.append("->");
            builder.append(out).append(", ");
        }
        for (String in : inputs) {
            if (!results.contains(in))
                builder.append("->").append(in).append(", ");
        }
        builder.setLength(builder.length()-2);
        return builder.toString();
    }

    @Override
    public boolean isProjected() {
        return modifiers().projection() != null;
    }

    @Override
    public @Nullable Set<Triple> getCachedMatchedTriples() {
        return getMatchedTriples();
    }

    protected @Nonnull String getPiWithNames() {
        return "π[" + getVarNamesStringContent() + "]";
    }

    protected @Nonnull String getVarNamesString() {
        return (isProjected() ? "π" : "") + "["+getVarNamesStringContent()+"]";
    }

    @Override
    public @Nonnull String toString() {
        return toString(new StringBuilder()).toString();
    }

    @Override
    public @Nonnull String prettyPrint() {
        return prettyPrint(new StringBuilder(), "").toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Op) ) return false;
        Op rhs = (Op) obj;
        return getClass().equals(rhs.getClass())
                && modifiers().equals(rhs.modifiers())
                && getChildren().equals(rhs.getChildren());
    }

    @Override
    public int hashCode() {
        return 37*(37*getClass().hashCode() + modifiers().hashCode()) + getChildren().hashCode();
    }
}
