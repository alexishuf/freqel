package br.ufsc.lapesd.freqel.reason.tbox.replacements;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.indexed.IndexSet;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

public class ReplacementContext {
    private static final Logger logger = LoggerFactory.getLogger(ReplacementContext.class);

    private final @Nonnull CQuery query;
    private final @Nonnull IndexSubset<Triple> triples;
    private @LazyInit @Nullable CQuery subQuery;
    private final @Nonnull TPEndpoint endpoint;

    public ReplacementContext(@Nonnull CQuery query, @Nonnull Triple triple,
                              @Nonnull TPEndpoint endpoint) {
        this(query, query.attr().getSet().subset(triple), endpoint);
    }

    public ReplacementContext(@Nonnull CQuery query, @Nonnull Collection<Triple> triples,
                              @Nonnull TPEndpoint endpoint) {
        assert !query.isEmpty();
        assert !triples.isEmpty();
        this.query = query;
        this.triples = toSubset(query.attr().getSet(), triples);
        this.endpoint = endpoint;
    }

    private IndexSubset<Triple> toSubset(@Nonnull IndexSet<Triple> parent,
                                         @Nonnull Collection<Triple> triples) {
        if (triples instanceof IndexSubset) {
            IndexSubset<Triple> ss = (IndexSubset<Triple>) triples;
            if (ss.getParent() == query.attr().getSet())
                return ss;
        }
        logger.debug("Bad performance: triples is not a IndexSubset<> of the query triples");
        return parent.immutableSubset(triples);
    }

    public @Nonnull CQuery getQuery() { return query; }
    public @Nonnull IndexSubset<Triple> getTriples() { return triples; }
    public @Nonnull TPEndpoint getEndpoint() { return endpoint; }

    public @Nonnull CQuery getSubQuery() {
        if (subQuery == null)
            subQuery = query.subQuery(triples);
        return subQuery;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ReplacementContext)) return false;
        ReplacementContext that = (ReplacementContext) o;
        return getQuery().equals(that.getQuery()) && getTriples().equals(that.getTriples()) && getEndpoint().equals(that.getEndpoint());
    }

    @Override public int hashCode() {
        return Objects.hash(getQuery(), getTriples(), getEndpoint());
    }

    @Override public @Nonnull String toString() {
        return String.format("Ctx{ep=%s, triples=%s}", endpoint, triples);
    }
}
