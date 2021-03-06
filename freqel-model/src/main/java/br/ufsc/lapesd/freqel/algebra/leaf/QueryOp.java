package br.ufsc.lapesd.freqel.algebra.leaf;

import br.ufsc.lapesd.freqel.algebra.AbstractOp;
import br.ufsc.lapesd.freqel.algebra.Op;
import br.ufsc.lapesd.freqel.model.RDFUtils;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.CQueryCache;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.modifiers.*;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

public class QueryOp extends AbstractOp {
    private MutableCQuery query = null;

    /**
     * Changing the modifiers implicitly updates many of the get*Vars() methods.
     * This delegate extends the updates done by CQuery internally to get*Vars()
     * methods not delegated to CQuery.
     *
     * Caches are not purged recursively, as the client code is responsible for that.
     * The cacheHit flag is not changed, as the client code may rely on its previous
     * value if it wishes to star an upwards recursive purge from this node.
     */
    private class ModifierSetInterceptor extends DelegatingModifiersSet {
        public ModifierSetInterceptor(@Nonnull ModifiersSet delegate) {
            super(delegate, false);
        }

        @Override protected void added(@Nonnull Modifier modifier) {
            super.added(modifier);
            if (modifier instanceof Projection || modifier instanceof Ask)
                strictResultVarsCache = publicVarsCache = allInputVarsCache = null;
        }

        @Override protected void removed(@Nonnull Modifier modifier) {
            super.added(modifier);
            if (modifier instanceof Projection || modifier instanceof Ask)
                strictResultVarsCache = publicVarsCache = allInputVarsCache = null;
        }
    }

    public QueryOp(@Nonnull CQuery query) {
        setQuery(query);
    }

    @Override
    public void offerTriplesUniverse(@Nonnull IndexSet<Triple> universe) {
        query.attr().offerTriplesUniverse(universe);
    }

    @Override public @Nullable IndexSet<Triple> getOfferedTriplesUniverse() {
        return query.attr().triplesUniverseOffer();
    }

    @Override public void offerVarsUniverse(@Nonnull IndexSet<String> universe) {
        if (query.attr().offerVarNamesUniverse(universe)) {
            purgeCachesShallow();
            //noinspection AssertWithSideEffects
            assert assertAllInvariants(true);
        }

    }

    @Override public @Nullable IndexSet<String> getOfferedVarsUniverse() {
        return query.attr().varNamesUniverseOffer();
    }

    @Override
    public @Nonnull ModifiersSet modifiers() {
        return new ModifierSetInterceptor(this.query.mutateModifiers());
    }

    public @Nonnull MutableCQuery getQuery() {
        assert query != null;
        return query;
    }

    public void setQuery(@Nonnull CQuery query) {
        if (query != this.query) {
            this.query = new MutableCQuery(query);
            CQueryCache attr = this.query.attr();
            IndexSet<String> vars = attr.varNamesUniverseOffer();
            if (vars != null)
                offerVarsUniverse(vars);
            IndexSet<Triple> triples = attr.triplesUniverseOffer();
            if (triples != null)
                offerTriplesUniverse(triples);
        }
    }

    public @Nonnull QueryOp withQuery(@Nonnull CQuery query) {
        return new QueryOp(query);
    }

    @Override
    public @Nonnull Set<String> getAllVars() {
        cacheHit = true;
        return query.attr().allVarNames();
    }
    @Override
    public @Nonnull Set<String> getResultVars() {
        cacheHit = true;
        boolean hasProjection = query.getModifiers().projection() != null;
        return hasProjection ? query.attr().publicVarNames() : query.attr().publicTripleVarNames();
    }
    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        cacheHit = true;
        return query.attr().reqInputVarNames();
    }
    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        cacheHit = true;
        return query.attr().optInputVarNames();
    }
    @Override
    public @Nonnull Set<String> getInputVars() {
        cacheHit = false;
        return query.attr().inputVarNames();
    }

    protected @Nonnull MutableCQuery bindQuery(@Nonnull Solution solution) {
        return getQuery().bind(RDFUtils.generalizeLiterals(solution));
    }

    protected @Nonnull QueryOp createWith(@Nonnull CQuery query) {
        return new QueryOp(query);
    }

    @Override
    public @Nonnull QueryOp createBound(@Nonnull Solution s) {
        return createWith(bindQuery(s));
    }

    @Override
    public @Nonnull  Op flatCopy() {
        QueryOp copy = createWith(new MutableCQuery(getQuery()));
        copy.setCardinality(getCardinality());
        copy.copyCaches(this);
        return copy;
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
        cacheHit = true;
        return getQuery().attr().matchedTriples();
    }

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        if (isProjected())
            builder.append(getPiWithNames()).append('(');
        builder.append("Q(").append(getQuery()).append(')');
        if (isProjected())
            builder.append(')');
        return builder;
    }

    protected @Nonnull StringBuilder prettyPrintQArgs(@Nonnull StringBuilder b) {
        return b;
    }

    @Override
    public @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                               @Nonnull String indent) {
        String indent2 = indent + "  ";
        builder.append(indent);
        if (isProjected())
            builder.append(getPiWithNames()).append('(');
        builder.append("Q(").append(getCardinality());
        prettyPrintQArgs(builder)
                .append(isProjected() ? ")) " : ")"+getVarNamesString()+" ")
                .append(getName()).append('\n').append(indent2);
        boolean hadModifier = false;
        for (Modifier modifier : modifiers()) {
            if (modifier instanceof Projection || modifier instanceof SPARQLFilter) continue;
            hadModifier = true;
            builder.append(modifier).append(", ");
        }
        if (hadModifier) {
            builder.setLength(builder.length()-2);
            builder.append('\n').append(indent2);
        }
        builder.append(getQuery().toString().replace("\n", "\n"+indent2));
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryOp)) return false;
        if (!super.equals(o)) return false;
        QueryOp that = (QueryOp) o;
        return Objects.equals(getQuery(), that.getQuery());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getQuery());
    }
}
