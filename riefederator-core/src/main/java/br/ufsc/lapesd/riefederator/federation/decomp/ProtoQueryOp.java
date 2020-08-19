package br.ufsc.lapesd.riefederator.federation.decomp;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.inner.UnionOp;
import br.ufsc.lapesd.riefederator.algebra.leaf.EndpointQueryOp;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.annotations.QueryRelevantTermAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.QueryRelevantTripleAnnotation;
import br.ufsc.lapesd.riefederator.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

public class ProtoQueryOp {
    private @Nonnull TPEndpoint endpoint;
    private @Nonnull CQuery matchedQuery;
    private @Nonnull Collection<CQuery> alternatives;
    private @Nullable Boolean queryRelevantAnnotations = null;

    public ProtoQueryOp(@Nonnull TPEndpoint endpoint, @Nonnull CQuery matchedQuery) {
        this(endpoint, matchedQuery, Collections.emptySet());
    }

    public ProtoQueryOp(@Nonnull TPEndpoint endpoint, @Nonnull CQuery matchedQuery,
                        @Nonnull Collection<CQuery> alternatives) {
        this.endpoint = endpoint;
        this.matchedQuery = matchedQuery;
        this.alternatives = alternatives;
    }

    public ProtoQueryOp(@Nonnull EndpointQueryOp queryOp) {
        this(queryOp.getEndpoint(), queryOp.getQuery());
    }

    public @Nonnull Op toOp() {
        if (!hasAlternatives()) {
            return new EndpointQueryOp(endpoint, matchedQuery);
        } else {
            UnionOp.Builder b = UnionOp.builder();
            b.add(new EndpointQueryOp(endpoint, matchedQuery));
            for (CQuery alternative : alternatives)
                b.add(new EndpointQueryOp(endpoint, alternative));
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

    public boolean hasAlternatives() {
        return alternatives.size() > 1;
    }

    public @Nonnull Collection<CQuery> getAlternatives() {
        return alternatives;
    }

    public void setAlternatives(@Nonnull Collection<CQuery> alternatives) {
        this.alternatives = alternatives;
    }

    public boolean hasQueryRelevantAnnotations() {
        if (queryRelevantAnnotations != null)
            return queryRelevantAnnotations;
        CQuery q = this.matchedQuery;
        queryRelevantAnnotations =
                   q.forEachTermAnnotation(QueryRelevantTermAnnotation.class, (t, a) -> {})
                || q.forEachTripleAnnotation(QueryRelevantTripleAnnotation.class, (t, a) -> {});
        return queryRelevantAnnotations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProtoQueryOp)) return false;
        ProtoQueryOp that = (ProtoQueryOp) o;
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
