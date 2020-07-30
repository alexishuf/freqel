package br.ufsc.lapesd.riefederator.rel.common;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.serializer.SerializationContext;

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
        StringBuilder b = replacingOperators(new StringBuilder(), ctx, filter, filter.getExpr());
        return b == null ? null : b.toString();
    }

    private @Nullable StringBuilder
    replacingOperators(@Nonnull StringBuilder b, @Nonnull SelectorFactory.Context ctx,
                       @Nonnull SPARQLFilter filter, @Nonnull Expr expr) {
        if (expr instanceof NodeValue) {
            String sql = termWriter.apply(fromJena(((NodeValue) expr).asNode()));
            return sql != null ? b.append(sql) : null;
        } else if (expr instanceof ExprVar) {
            Term term = filter.getVar2Term().get(expr.getVarName());
            assert term != null;
            Var var = term.asVar();
            Column column = ctx.getDirectMapped(var, null);
            assert column != null;
            return b.append(column);
        } else if (expr instanceof ExprNone || expr instanceof ExprAggregator) {
            return b;
        }

        assert expr instanceof ExprFunction;
        ExprFunction function = (ExprFunction) expr;
        String name = function.getOpName();
        if (name == null)
            name = function.getFunctionName(new SerializationContext());

        String sql = sparqlOp2RelOp.get(name);
        if (sql == null)
            return null; //abort
        b.append('(');
        if (function.numArgs() == 2) {
            if (replacingOperators(b, ctx, filter, function.getArg(1)) == null)
                return null;
            b.append(' ').append(sql).append(' ');
            if (replacingOperators(b, ctx, filter, function.getArg(2)) != null)
                return b.append(')');
        } else if (function.numArgs() == 1) {
            b.append(sql).append(' ');
            if (replacingOperators(b, ctx, filter, function.getArg(1)) != null)
                return b.append(')');
        }
        return null;
    }
}
