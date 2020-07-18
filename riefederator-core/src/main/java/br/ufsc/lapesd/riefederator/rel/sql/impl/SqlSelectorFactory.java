package br.ufsc.lapesd.riefederator.rel.sql.impl;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.Selector;
import br.ufsc.lapesd.riefederator.rel.common.SelectorFactory;
import br.ufsc.lapesd.riefederator.rel.common.StarsHelper;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.sql.SqlTermWriter;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.serializer.SerializationContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static java.util.Arrays.asList;

public class SqlSelectorFactory implements SelectorFactory {
    private static final @Nonnull Map<String, String> sparqlOp2sqlOp;

    static {
        Map<String, String> map = new HashMap<>();
        asList("<", "<=", "=", ">=", ">", "+", "-", "*", "/").forEach(o -> map.put(o, o));
        map.put("!=", "<>");
        map.put("&&", "AND");
        map.put("||", "OR");
        map.put("!", "NOT");
        sparqlOp2sqlOp = map;
    }

    private final @Nonnull SqlTermWriter termWriter;

    public SqlSelectorFactory(@Nonnull SqlTermWriter termWriter) {
        this.termWriter = termWriter;
    }

    /* --- --- --- Internals  --- --- --- */

    private StringBuilder replacingOperators(@Nonnull StringBuilder b, @Nonnull Context ctx,
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

        String sql = sparqlOp2sqlOp.get(name);
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

    /*  --- --- --- Interface  --- --- --- */

    @Override
    public @Nullable Selector create(@Nonnull Context context, @Nonnull SPARQLFilter filter) {
        List<Term> vars = new ArrayList<>();
        List<Column> columns = new ArrayList<>();
        for (Var var : filter.getVarTerms()) {
            Column column = context.getDirectMapped(var, null);
            if (column == null) return null;
            vars.add(var);
            columns.add(column);
        }

        StringBuilder builder = replacingOperators(new StringBuilder(), context,
                                                   filter, filter.getExpr());
        if (builder != null)
            return new FilterSelector(columns, vars, builder.toString());
        return null;
    }

    @Override
    public @Nullable Selector create(@Nonnull Context ctx, @Nonnull Triple triple) {
        Term o = triple.getObject();
        Column column = ctx.getDirectMapped(o, triple);
        if (column == null) {
            assert !ctx.getColumns(o).isEmpty() || StarsHelper.isPostRelational(ctx.getQuery(), o)
                    : o+" is not marked as post relational nor has any Column bound";
            return null;
        }
        if (o.isGround()) return new EqualsSqlSelector(column, o);
        if (o.isBlank()) return new ExistsSqlSelector(column, o.asBlank());
        return null;
    }
}
