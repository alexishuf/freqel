package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.jena.ExprUtils;
import br.ufsc.lapesd.riefederator.jena.model.term.node.JenaVarNode;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import org.apache.jena.graph.Node_Variable;
import org.apache.jena.sparql.expr.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;

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
        return state.visit(filter.getExpr()) ? state.b.toString() : null;
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

        public boolean visit(@Nonnull Expr expr) {
            if (expr instanceof NodeValue) {
                return visitValue((NodeValue) expr);
            } else if (expr instanceof ExprVar) {
                return visitVar(expr);
            } else if (expr instanceof ExprNone || expr instanceof ExprAggregator) {
                return true;
            }

            assert expr instanceof ExprFunction;
            return visitFunction((ExprFunction) expr);
        }

        public boolean visitFunction(@Nonnull ExprFunction expr) {
            String sql = sparqlOp2RelOp.get(ExprUtils.getFunctionName(expr));
            if (sql == null)
                return false; //abort
            return visitRelOp(expr, sql);
        }

        public boolean visitRelOp(@Nonnull ExprFunction expr, @Nonnull String sqlOp) {
            b.append('(');
            if (expr.numArgs() == 2) {
                if (!visit(expr.getArg(1)))
                    return false;
                b.append(' ').append(sqlOp).append(' ');
                if (visit(expr.getArg(2))) {
                    b.append(')');
                    return true;
                }
            } else if (expr.numArgs() == 1) {
                b.append(sqlOp).append(' ');
                if (visit(expr.getArg(1))) {
                    b.append(')');
                    return true;
                }
            }
            return true;
        }

        public boolean visitVar(@Nonnull Expr expr) {
            Column column = getColumn((ExprVar) expr);
            b.append(column);
            return true;
        }

        protected @Nonnull Column getColumn(@Nonnull ExprVar expr) {
            JenaVarNode var = new JenaVarNode((Node_Variable) expr.getAsNode());
            Column column = ctx.getDirectMapped(var, null);
            assert  column != null;
            return column;
        }

        public boolean visitValue(NodeValue expr) {
            String sql = termWriter.apply(fromJena(expr.asNode()));
            boolean ok = sql != null;
            if (ok)
                b.append(sql);
            return ok;
        }
    }
}
