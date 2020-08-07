package br.ufsc.lapesd.riefederator.algebra.leaf;

import br.ufsc.lapesd.riefederator.algebra.AbstractOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.CQueryData;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.Set;

public class UnassignedQueryOp extends AbstractOp {
    protected MutableCQuery query = null;

    public UnassignedQueryOp(@Nonnull CQuery query) {
        setQuery(query);
    }

    @Override
    public @Nonnull ModifiersSet modifiers() {
        return query.mutateModifiers();
    }

    public @Nonnull CQuery getQuery() {
        return query;
    }

    public void setQuery(@Nonnull CQuery query) {
        if (this.query != null)
            this.query.mutateModifiers().removeListener(modifiersListener);
        this.query = new MutableCQuery(query) {
            @Override
            protected void makeExclusive() {
                CQueryData exclusive = d.toExclusive().attach();
                if (exclusive != d) {
                    d.modifiers.removeListener(modifiersListener);
                    d = exclusive;
                    d.modifiers.addListener(modifiersListener);
                }
            }
        };
        this.query.mutateModifiers().addListener(modifiersListener);
    }

    @Override
    public @Nonnull Set<String> getAllVars() {
        return query.attr().allVarNames();
    }
    @Override
    public @Nonnull Set<String> getResultVars() {
        return query.attr().publicTripleVarNames();
    }
    @Override
    public @Nonnull Set<String> getRequiredInputVars() {
        return query.attr().reqInputVarNames();
    }
    @Override
    public @Nonnull Set<String> getOptionalInputVars() {
        return query.attr().optInputVarNames();
    }
    @Override
    public @Nonnull Set<String> getInputVars() {
        return query.attr().inputVarNames();
    }

    @Contract("_, _, !null -> !null")
    protected Term bind(@Nonnull Term term, @Nonnull Solution solution, Term fallback) {
        return term.isVar() ? solution.get(term.asVar().getName(), fallback) : fallback;
    }

    protected @Nonnull Triple bind(@Nonnull Triple t, @Nonnull Solution solution) {
        Term s = bind(t.getSubject(), solution, null);
        Term p = bind(t.getPredicate(), solution, null);
        Term o = bind(t.getObject(), solution, null);
        if (s == null && p == null && o == null)
            return t;
        return new Triple(s == null ? t.getSubject()   : s,
                          p == null ? t.getPredicate() : p,
                          o == null ? t.getObject()    : o);
    }

    protected @Nonnull MutableCQuery bindQuery(@Nonnull Solution s) {
        CQuery q = getQuery();
        MutableCQuery b = new MutableCQuery();
        for (Triple triple : q) {
            Triple bound = bind(triple, s);
            b.add(bound);
            q.getTripleAnnotations(triple).forEach(a -> b.annotate(bound, a));
        }
        TreeUtils.addBoundModifiers(b.mutateModifiers(), q.getModifiers(), s);
        IndexedSet<Term> tripleTerms = q.attr().tripleTerms();
        q.forEachTermAnnotation((t, a) -> {
            Term boundTerm = bind(t, s, t);
            if (tripleTerms.contains(t)) b.annotate(boundTerm, a);
            else                         b.annotate(SPARQLFilter.generalizeAsBind(boundTerm), a);
        });
        return b;
    }

    @Override
    public @Nonnull UnassignedQueryOp createBound(@Nonnull Solution s) {
        return new UnassignedQueryOp(bindQuery(s));
    }

    @Override
    public @Nonnull Set<Triple> getMatchedTriples() {
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

    @Override
    public  @Nonnull StringBuilder prettyPrint(@Nonnull StringBuilder builder,
                                               @Nonnull String indent) {
        String indent2 = indent + "  ";
        builder.append(indent);
        if (isProjected())
            builder.append(getPiWithNames()).append('(');
        builder.append("Q(")
                .append(isProjected() ? ")) " : ")"+getVarNamesString()+" ")
                .append(getName()).append('\n').append(indent2)
                .append(getQuery().toString().replace("\n", "\n"+indent2));
        printFilters(builder, indent2);
        return builder;
    }
}
