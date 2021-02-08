package br.ufsc.lapesd.freqel.algebra.leaf;

import br.ufsc.lapesd.freqel.model.RDFUtils;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.annotations.OverrideAnnotation;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.util.indexed.subset.IndexSubset;

import javax.annotation.Nonnull;

import static br.ufsc.lapesd.freqel.algebra.util.TreeUtils.addBoundModifiers;

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

    @Override protected @Nonnull MutableCQuery bindQuery(@Nonnull Solution solution) {
        if (!endpoint.requiresBindWithOverride())
            return super.bindQuery(solution);
        solution = RDFUtils.generalizeLiterals(solution);
        MutableCQuery original = getQuery();
        MutableCQuery boundQuery = new MutableCQuery(original);
        boundQuery.mutateModifiers().removeAll(original.getModifiers().filters());
        addBoundModifiers(boundQuery.mutateModifiers(), original.getModifiers(), solution);

        IndexSubset<Var> filterVars = original.attr().allVars().fullSubset();
        filterVars.removeAll(original.attr().tripleVars());
        for (Var filterVar : filterVars) {
            if (solution.get(filterVar) != null)
                boundQuery.deannotate(filterVar);
        }

        for (Var var : boundQuery.attr().allVars()) {
            Term bound = solution.get(var);
            if (bound != null) {
                boundQuery.deannotateTermIf(var, OverrideAnnotation.class::isInstance);
                boundQuery.annotate(var, new OverrideAnnotation(bound));
            }
        }
        return boundQuery;
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

    @Override protected @Nonnull StringBuilder prettyPrintQArgs(@Nonnull StringBuilder b) {
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
