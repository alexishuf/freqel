package br.ufsc.lapesd.riefederator.federation.tree.proto;

import br.ufsc.lapesd.riefederator.federation.tree.MultiQueryNode;
import br.ufsc.lapesd.riefederator.federation.tree.PlanNode;
import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

public class ProtoQueryNode {
    private @Nonnull TPEndpoint endpoint;
    private @Nonnull CQuery matchedQuery;
    private @Nonnull Collection<CQuery> alternatives;

    public ProtoQueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery matchedQuery) {
        this(endpoint, matchedQuery, Collections.emptySet());
    }

    public ProtoQueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery matchedQuery,
                          @Nonnull Collection<CQuery> alternatives) {
        this.endpoint = endpoint;
        this.matchedQuery = matchedQuery;
        this.alternatives = alternatives;
    }

    public ProtoQueryNode(@Nonnull QueryNode queryNode) {
        this(queryNode.getEndpoint(), queryNode.getQuery());
    }

    public @Nonnull PlanNode toNode() {
        if (alternatives.isEmpty()) {
            return new QueryNode(endpoint, matchedQuery);
        } else {
            MultiQueryNode.Builder b = MultiQueryNode.builder();
            b.add(new QueryNode(endpoint, matchedQuery));
            for (CQuery alternative : alternatives)
                b.add(new QueryNode(endpoint, alternative));
            return b.build();
        }
    }

    public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(@Nonnull TPEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public @Nonnull CQuery getMatchedQuery() {
        return matchedQuery;
    }

    public void setMatchedQuery(@Nonnull CQuery matchedQuery) {
        this.matchedQuery = matchedQuery;
    }

    public @Nonnull Collection<CQuery> getAlternatives() {
        return alternatives;
    }

    public void setAlternatives(@Nonnull Collection<CQuery> alternatives) {
        this.alternatives = alternatives;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProtoQueryNode)) return false;
        ProtoQueryNode that = (ProtoQueryNode) o;
        return getEndpoint().equals(that.getEndpoint()) &&
                getMatchedQuery().equals(that.getMatchedQuery()) &&
                getAlternatives().equals(that.getAlternatives());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getEndpoint(), getMatchedQuery(), getAlternatives());
    }

    @Override
    public @Nonnull String toString() {
        return String.format("ProtoQueryNode(%s, %s, %s)",
                             getEndpoint(), getMatchedQuery(), getAlternatives());
    }
}
