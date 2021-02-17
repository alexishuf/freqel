package br.ufsc.lapesd.freqel.reason.tbox.replacements;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

public class Replacement {
    private final @Nonnull Term term;
    private final @Nonnull Set<Term> alternatives;
    private final @Nonnull ReplacementContext ctx;

    public Replacement(@Nonnull Term term, @Nonnull Set<Term> alternatives, @Nonnull CQuery ctxQuery,
                       @Nonnull Collection<Triple> ctxTriples, @Nonnull TPEndpoint endpoint) {
        this(term, alternatives, new ReplacementContext(ctxQuery, ctxTriples, endpoint));
    }
    public Replacement(@Nonnull Term term, @Nonnull Set<Term> alternatives,
                       @Nonnull ReplacementContext ctx) {
        this.term = term;
        this.alternatives = alternatives;
        this.ctx = ctx;
    }

    public @Nonnull Term getTerm() {
        return term;
    }

    public @Nonnull Set<Term> getAlternatives() {
        return alternatives;
    }

    public @Nonnull ReplacementContext getCtx() {
        return ctx;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Replacement)) return false;
        Replacement that = (Replacement) o;
        return getTerm().equals(that.getTerm()) && getAlternatives().equals(that.getAlternatives()) && getCtx().equals(that.getCtx());
    }

    @Override public int hashCode() {
        return Objects.hash(getTerm(), getAlternatives(), getCtx());
    }

    @Override public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("Replacement{").append(term.toString()).append(" -> {");
        int count = 0;
        for (Term alternative : alternatives) {
            b.append(alternative).append(", ");
            ++count;
            if (b.length() > 70 && alternatives.size() > count) {
                b.append("... ").append(alternatives.size()-count).append(" more, ");
                break;
            }
        }
        if (count > 0)
            b.setLength(b.length()-2);
        return b.append("}, ctx=").append(ctx).append('}').toString();
    }
}
