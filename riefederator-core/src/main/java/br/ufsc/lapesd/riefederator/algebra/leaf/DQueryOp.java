package br.ufsc.lapesd.riefederator.algebra.leaf;

import br.ufsc.lapesd.riefederator.algebra.AbstractOp;
import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.query.endpoint.DQEndpoint;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

public class DQueryOp extends AbstractOp implements EndpointOp {
    protected @Nonnull DQEndpoint endpoint;
    protected @Nonnull Op query;

    public DQueryOp(@Nonnull DQEndpoint endpoint, @Nonnull Op query) {
        this.endpoint = endpoint;
        this.query = query;
        query.attachTo(this);
        assertAllInvariants();
    }

    @Override public @Nonnull DQEndpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(@Nonnull DQEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public @Nonnull Op getQuery() {
        return query;
    }

    public void setQuery(@Nonnull Op query) {
        if (this.query != query) {
            this.query.detachFrom(this);
            this.query = query;
            query.attachTo(this);
        }
        assertAllInvariants();
    }

    /* --- --- --- non-delegated interface methods --- --- --- */

    @Override public @Nonnull ModifiersSet modifiers() {
        return query.modifiers();
    }

    @Override public @Nonnull Op createBound(@Nonnull Solution solution) {
        return new DQueryOp(endpoint, query.createBound(solution));
    }

    @Override public @Nonnull Op flatCopy() {
        DQueryOp copy = new DQueryOp(endpoint, TreeUtils.deepCopy(query));
        copy.copyCaches(this);
        copy.setCardinality(getCardinality());
        return copy;
    }

    @Override public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        if (isProjected())
            builder.append(getPiWithNames()).append('(');
        builder.append("DQ(").append(getQuery()).append(')');
        if (isProjected())
            builder.append(')');
        return builder;
    }

    @Override
    public @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                              @Nonnull String indent) {
        builder.append(indent).append("DQ\n");
        return query.prettyPrint(builder, indent + "  ");
    }


    /* --- --- --- interface methods delegated on fallback --- --- --- */

    @Override public @Nonnull Cardinality getCardinality() {
        Cardinality cardinality = super.getCardinality();
        return cardinality == Cardinality.UNSUPPORTED ? query.getCardinality() : cardinality;
    }


    /* --- --- --- delegated interface methods --- --- --- */

    @Override
    public void offerTriplesUniverse(@Nonnull IndexSet<Triple> universe) {
        query.offerTriplesUniverse(universe);
    }

    @Override public @Nullable IndexSet<Triple> getOfferedTriplesUniverse() {
        return query.getOfferedTriplesUniverse();
    }

    @Override public void offerVarsUniverse(@Nonnull IndexSet<String> universe) {
        query.offerVarsUniverse(universe);
    }

    @Override public @Nullable IndexSet<String> getOfferedVarsUniverse() {
        return query.getOfferedVarsUniverse();
    }

    @Override public @Nonnull Set<String> getResultVars() {
        return query.getResultVars();
    }

    @Override public @Nonnull Set<String> getAllVars() {
        return query.getAllVars();
    }

    @Override public @Nonnull Set<String> getPublicVars() {
        return query.getPublicVars();
    }

    @Override public @Nonnull Set<String> getStrictResultVars() {
        return query.getStrictResultVars();
    }

    @Override public @Nonnull Set<String> getInputVars() {
        return query.getInputVars();
    }

    @Override public @Nonnull Set<String> getRequiredInputVars() {
        return query.getRequiredInputVars();
    }

    @Override public @Nonnull Set<String> getOptionalInputVars() {
        return query.getOptionalInputVars();
    }

    @Override public boolean hasInputs() {
        return query.hasInputs();
    }

    @Override public boolean hasRequiredInputs() {
        return query.hasRequiredInputs();
    }

    @Override public boolean isProjected() {
        return query.isProjected();
    }

    @Override public @Nonnull Set<Triple> getMatchedTriples() {
        return query.getMatchedTriples();
    }

    @Override public @Nullable Set<Triple> getCachedMatchedTriples() {
        return query.getCachedMatchedTriples();
    }
}
