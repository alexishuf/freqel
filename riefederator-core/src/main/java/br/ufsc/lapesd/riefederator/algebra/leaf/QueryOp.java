package br.ufsc.lapesd.riefederator.algebra.leaf;

import br.ufsc.lapesd.riefederator.algebra.AbstractOp;
import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.algebra.OpChangeListener;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.CQueryData;
import br.ufsc.lapesd.riefederator.query.MutableCQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.riefederator.query.modifiers.Projection;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

public class QueryOp extends AbstractOp {
    protected MutableCQuery query = null;

    public QueryOp(@Nonnull CQuery query) {
        setQuery(query);
    }

    @Override
    public @Nonnull ModifiersSet modifiers() {
        return query.mutateModifiers();
    }

    public @Nonnull MutableCQuery getQuery() {
        return query;
    }

    public void setQuery(@Nonnull CQuery query) {
        if (query != this.query) {
            if (this.query != null)
                this.query.mutateModifiers().removeListener(modifiersListener);
            this.query = new MutableCQuery(query) {
                @Override
                protected void makeExclusive() {
                    CQueryData exclusive = d.toExclusive();
                    if (exclusive != d) {
                        d.modifiers.removeListener(modifiersListener);
                        d = exclusive;
                        d.modifiers.addListener(modifiersListener);
                    }
                }
            };
            this.query.mutateModifiers().addListener(modifiersListener);
        }
        for (OpChangeListener listener : listeners)
            listener.matchedTriplesChanged(this);
        notifyVarsChanged();
    }

    public @Nonnull QueryOp withQuery(@Nonnull CQuery query) {
        return new QueryOp(query);
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

    protected @Nonnull MutableCQuery bindQuery(@Nonnull Solution solution) {
        Solution s = RDFUtils.generalizeLiterals(solution);
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
            else                         b.annotate(boundTerm, a);
        });
        return b;
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
        return copy;
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
