package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.util.CollectionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.riefederator.util.CollectionUtils.union;
import static java.util.Objects.requireNonNull;

public abstract class AbstractOp implements Op {
    protected @Nullable Set<String> strictResultVarsCache, publicVarsCache, allInputVarsCache;
    protected @Nonnull String name;
    private @Nonnull Cardinality cardinality = Cardinality.UNSUPPORTED;
    protected @Nonnull List<Op> parents = new ArrayList<>();
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
        boolean oldCacheHit = this.cacheHit;
        Projection projection = modifiers().projection();
        assert projection == null || projection.getVarNames().equals(getResultVars());
        assert getPublicVars().containsAll(getResultVars());
        assert getPublicVars().containsAll(getInputVars());
        assert getInputVars().containsAll(getRequiredInputVars());
        assert getInputVars().containsAll(getOptionalInputVars());
        assert getResultVars().containsAll(getStrictResultVars());
        assert getStrictResultVars().stream().noneMatch(getInputVars()::contains);
        purgeCachesShallow();
        cacheHit = oldCacheHit;
        return true;
    }

    @Override
    public @Nonnull Set<String> getStrictResultVars() {
        if (strictResultVarsCache == null) {
            cacheHit = true;
            if (hasInputs())
                strictResultVarsCache = CollectionUtils.setMinus(getResultVars(), getInputVars());
            else
                strictResultVarsCache = getResultVars();
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
        return parents;
    }

    @Override
    public void attachTo(@Nonnull Op parent) {
        if (parent == this) throw new IllegalArgumentException("Node cannot be its own parent");
        assert parents.stream().noneMatch(o -> o == parent) : "already attached to this parent";
        parents.add(parent);
    }

    @Override
    public void detachFrom(@Nonnull Op parent) {
        boolean found = false;
        for (Iterator<Op> it = parents.iterator(); it.hasNext(); ) {
            if (it.next() == parent) {
                found = true;
                it.remove();
                break;
            }
        }
        assert found : "parent does not appear in parents list";
        assert parents.stream().noneMatch(o -> o == parent) : "parent appears twice in parents list";
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
