package br.ufsc.lapesd.freqel.rel.common;

import br.ufsc.lapesd.freqel.jena.query.modifiers.filter.JenaSPARQLFilter;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilter;
import br.ufsc.lapesd.freqel.query.modifiers.filter.SPARQLFilterNode;
import br.ufsc.lapesd.freqel.rel.mappings.Column;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

import static br.ufsc.lapesd.freqel.jena.JenaWrappers.fromJena;

public class FilterOperatorRewriter {
    private @Nonnull final Map<String, String> sparqlOp2RelOp;
    private @Nonnull final RelationalTermWriter termWriter;

    public FilterOperatorRewriter(@Nonnull Map<String, String> sparqlOp2RelOp,
                                  @Nonnull RelationalTermWriter termWriter) {
        this.sparqlOp2RelOp = sparqlOp2RelOp;
        this.termWriter = termWriter;
    }

    public @Nullable String rewrite(@Nonnull SelectorFactory.Context ctx,
                                    @Nonnull SPARQLFilter filter) {
        State state = createState(new StringBuilder(), ctx, filter);
        return state.visit(JenaSPARQLFilter.build(filter).getExpr()) ? state.b.toString() : null;
    }

    protected @Nonnull State createState(@Nonnull StringBuilder b,
                                         @Nonnull SelectorFactory.Context ctx,
                                         @Nonnull SPARQLFilter filter) {
        return new State(b, ctx, filter);
    }

    protected class State {
        protected @Nonnull final StringBuilder b;
        protected @Nonnull final SelectorFactory.Context ctx;
        protected @Nonnull final SPARQLFilter filter;

        public State(@Nonnull StringBuilder b, @Nonnull SelectorFactory.Context ctx,
                     @Nonnull SPARQLFilter filter) {
            this.b = b;
            this.ctx = ctx;
            this.filter = filter;
        }

        public boolean visit(@Nonnull SPARQLFilterNode expr) {
            if (expr.isTerm()) {
                Term t = Objects.requireNonNull(expr.asTerm());
                if (t.isVar())
                    return visitVar(t.asVar());
                else
                    return visitValue(t);
            } else {
                return visitFunction(expr);
            }
        }

        public boolean visitFunction(@Nonnull SPARQLFilterNode expr) {
            String sql = sparqlOp2RelOp.get(expr.name());
            if (sql == null)
                return false; //abort
            return visitRelOp(expr, sql);
        }

        public boolean visitRelOp(@Nonnull SPARQLFilterNode expr, @Nonnull String sqlOp) {
            b.append('(');
            if (expr.argsCount() == 2) {
                if (!visit(expr.arg(0)))
                    return false;
                b.append(' ').append(sqlOp).append(' ');
                if (visit(expr.arg(1))) {
                    b.append(')');
                    return true;
                }
            } else if (expr.argsCount() == 1) {
                b.append(sqlOp).append(' ');
                if (visit(expr.arg(0))) {
                    b.append(')');
                    return true;
                }
            }
            return true;
        }

        public boolean visitVar(@Nonnull Var var) {
            Column column = getColumn(var);
            b.append(column);
            return true;
        }

        protected @Nonnull Column getColumn(@Nonnull Var var) {
            Column column = ctx.getDirectMapped(var, null);
            assert  column != null;
            return column;
        }

        public boolean visitValue(@Nonnull Term term) {
            String sql = termWriter.apply(term);
            boolean ok = sql != null;
            if (ok)
                b.append(sql);
            return ok;
        }
    }
}
