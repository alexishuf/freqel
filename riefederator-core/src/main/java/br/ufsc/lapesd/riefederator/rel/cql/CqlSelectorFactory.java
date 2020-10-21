package br.ufsc.lapesd.riefederator.rel.cql;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.annotations.OverrideAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.FilterOperatorRewriter;
import br.ufsc.lapesd.riefederator.rel.common.RelationalTermWriter;
import br.ufsc.lapesd.riefederator.rel.common.Selector;
import br.ufsc.lapesd.riefederator.rel.common.SelectorFactory;
import br.ufsc.lapesd.riefederator.rel.cql.impl.CqlFilterOperatorRewriter;
import br.ufsc.lapesd.riefederator.rel.cql.impl.EqualsCqlSelector;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.sql.impl.FilterSelector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class CqlSelectorFactory implements SelectorFactory {
    private @Nonnull final FilterOperatorRewriter filterRw;
    private @Nonnull final RelationalTermWriter termWriter;

    public CqlSelectorFactory(@Nonnull RelationalTermWriter termWriter) {
        filterRw = new CqlFilterOperatorRewriter(termWriter);
        this.termWriter = termWriter;
    }

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
        if (column != null) {
            assert ctx.getQuery().getTermAnnotations(o).stream()
                      .filter(OverrideAnnotation.class::isInstance).count() <= 1;
            for (TermAnnotation ann : ctx.getQuery().getTermAnnotations(o)) {
                if (ann instanceof OverrideAnnotation)
                    o = ((OverrideAnnotation) ann).getValue();
            }
            if (o.isGround())
                return EqualsCqlSelector.create(column, o, termWriter);
        }
        return null;
    }
}
