package br.ufsc.lapesd.freqel.rel.sql.impl;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.query.annotations.OverrideAnnotation;
import br.ufsc.lapesd.freqel.query.annotations.TermAnnotation;
import br.ufsc.lapesd.freqel.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.freqel.rel.common.*;
import br.ufsc.lapesd.freqel.rel.mappings.Column;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private @Nonnull final FilterOperatorRewriter filterRw;

    public SqlSelectorFactory(@Nonnull RelationalTermWriter termWriter) {
        filterRw = new FilterOperatorRewriter(sparqlOp2sqlOp, termWriter);
    }

    /*  --- --- --- Interface  --- --- --- */

    @Override
    public @Nullable Selector create(@Nonnull Context context, @Nonnull SPARQLFilter filter) {
        List<Term> vars = new ArrayList<>();
        List<Column> columns = new ArrayList<>();
        for (Var var : filter.getVars()) {
            Column column = context.getDirectMapped(var, null);
            if (column == null) return null;
            vars.add(var);
            columns.add(column);
        }

        String rewritten = filterRw.rewrite(context, filter);
        return rewritten != null ? new FilterSelector(columns, vars, rewritten) : null;
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
        assert ctx.getQuery().getTermAnnotations(o).stream()
                  .filter(OverrideAnnotation.class::isInstance).count() <= 1;
        for (TermAnnotation ann : ctx.getQuery().getTermAnnotations(o)) {
            if (ann instanceof OverrideAnnotation)
                o = ((OverrideAnnotation) ann).getValue();
        }
        if (o.isGround()) return new EqualsSqlSelector(column, o);
        if (o.isBlank()) return new ExistsSqlSelector(column, o.asBlank());
        return null;
    }
}
