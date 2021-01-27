package br.ufsc.lapesd.freqel.algebra.leaf;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.annotations.OverrideAnnotation;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.results.Solution;

import javax.annotation.Nonnull;

public class EndpointQueryOp extends QueryOp implements EndpointOp {
    protected final @Nonnull TPEndpoint endpoint;

    public EndpointQueryOp(@Nonnull TPEndpoint endpoint, @Nonnull CQuery query) {
        super(query);
        this.endpoint = endpoint;
        assertAllInvariants();
    }

    @Override public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }

    @Override
    public @Nonnull QueryOp withQuery(@Nonnull CQuery query) {
        return new EndpointQueryOp(endpoint, query);
    }

    @Override
    protected @Nonnull QueryOp createWith(@Nonnull CQuery query) {
        return new EndpointQueryOp(getEndpoint(), query);
    }

    @Override
    protected Term bind(@Nonnull Term term, @Nonnull Solution solution, Term fallback) {
        return endpoint.requiresBindWithOverride() ? term : super.bind(term, solution, fallback);
    }

    @Override
    protected void annotationBind(@Nonnull Term term, @Nonnull Solution solution, Term fallback,
                                  @Nonnull MutableCQuery query) {
        if (endpoint.requiresBindWithOverride()) {
            Term bound = super.bind(term, solution, fallback);
            if (bound != fallback) {
                query.deannotateTermIf(term, OverrideAnnotation.class::isInstance);
                query.annotate(term, new OverrideAnnotation(bound));
            }
        }
    }

    @Override
    public @Nonnull StringBuilder toString(@Nonnull StringBuilder builder) {
        if (isProjected())
            builder.append(getPiWithNames()).append('(');
        builder.append("Q(").append(getEndpoint()).append(", ").append(getQuery()).append(')');
        if (isProjected())
            builder.append(')');
        return builder;
    }

    @Nonnull @Override protected StringBuilder prettyPrintQArgs(@Nonnull StringBuilder b) {
        return b.append(' ').append(getEndpoint());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && getEndpoint().equals(((EndpointQueryOp)obj).getEndpoint());
    }

    @Override
    public int hashCode() {
        return 37*super.hashCode() + getEndpoint().hashCode();
    }
}
