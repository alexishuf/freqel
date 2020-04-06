package br.ufsc.lapesd.riefederator.federation.tree.proto;

import br.ufsc.lapesd.riefederator.federation.tree.QueryNode;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import java.util.Objects;

public class ProtoQueryNode {
    private @Nonnull TPEndpoint endpoint;
    private @Nonnull CQuery query;

    public ProtoQueryNode(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query) {
        this.endpoint = endpoint;
        this.query = query;
    }

    public ProtoQueryNode(@Nonnull QueryNode queryNode) {
        this(queryNode.getEndpoint(), queryNode.getQuery());
    }

    public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(@Nonnull TPEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public @Nonnull CQuery getQuery() {
        return query;
    }

    public void setQuery(@Nonnull CQuery query) {
        this.query = query;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProtoQueryNode)) return false;
        ProtoQueryNode that = (ProtoQueryNode) o;
        return endpoint.equals(that.endpoint) &&
                query.equals(that.query);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpoint, query);
    }

    @Override
    public @Nonnull String toString() {
        return String.format("ProtoQueryNode(%s, %s)", getEndpoint(), getQuery());
    }
}
