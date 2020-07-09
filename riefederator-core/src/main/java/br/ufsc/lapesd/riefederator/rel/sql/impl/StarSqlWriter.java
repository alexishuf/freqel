package br.ufsc.lapesd.riefederator.rel.sql.impl;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.StarSubQuery;
import br.ufsc.lapesd.riefederator.rel.common.StarVarIndex;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.sql.SqlTermWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.serializer.SerializationContext;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.*;

import static br.ufsc.lapesd.riefederator.jena.JenaWrappers.fromJena;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;

@NotThreadSafe
public class StarSqlWriter {
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

    /* --- --- --- cached data structures: avoid repeated allocation --- --- --- */
    private final @Nonnull ArrayList<SqlSelector> selectors = new ArrayList<>();
    private final @Nonnull Map<String, Column> var2col = new HashMap<>();
    private final @Nonnull HashSet<String> tableSet = new HashSet<>();
    private final @Nonnull HashSet<String> chosenTableSet = new HashSet<>();
    private final @Nonnull HashSet<Column> columnSet = new HashSet<>();
    private @Nonnull CQuery query = CQuery.EMPTY;
    private @Nonnull String table = "";
    private @Nonnull Term core = new StdURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#nil");
    private StarSubQuery star;
    private StarVarIndex index;
    private final @Nonnull SqlTermWriter termWriter;

    public StarSqlWriter(@Nonnull SqlTermWriter termWriter) {
        this.termWriter = termWriter;
    }

    /* --- --- --- public interface --- --- --- */

    /**
     * Write the starIdx-th star in varIndex as SQL. FILTERS incorporated in the SQL are
     * removed from pendingFilters.
     *
     * If the WHERE clause is empty, the output will be <code>Table AS star_{starIdx}</code>.
     * Else, the output will be something like:
     * <code>
     *     (SELECT T.usedCol AS sparqlVar, ... FROM T WHERE T.usedCol = {sparqlValue} AND ...)
     *     AS star_{starIdx}
     * </code>
     *
     * @param b output builder
     * @param varIndex The index that contains the star and its variable dependencies
     * @param starIdx The index of the star within varIndex
     * @param pendingFilters Set of FILTERs that need processing. FILTERS encoded into SQL
     *                       will be removed if this parameter is not null.
     * @return <code>b</code>
     */
    @CanIgnoreReturnValue
    public @Nonnull StringBuilder write(@Nonnull StringBuilder b, @Nonnull StarVarIndex varIndex,
                                        int starIdx,
                                        @Nullable Set<SPARQLFilter> pendingFilters) {
        StarSubQuery star = setUp(varIndex, starIdx);
        setUpFilters(pendingFilters, star);
        return writeSql(b, varIndex, starIdx);
    }

    /**
     * Convenience version of {@link StarSqlWriter#write(StringBuilder, StarVarIndex, int, Set)}.
     */
    @CheckReturnValue
    public @Nonnull String write(@Nonnull StarVarIndex varIndex, int starIdx,
                                 @Nullable Set<SPARQLFilter> pendingFilters) {
        return write(new StringBuilder(), varIndex, starIdx, pendingFilters).toString();
    }

    /* --- --- --- sub-tasks --- --- --- */
    private void setUp(@Nonnull StarSubQuery star) {
        this.star = star;
        core = star.getCore();
        query = star.getQuery();
        String found = star.findTable();
        if (found == null)
            throw new IllegalArgumentException("Star has no TableTag!");
        this.table = found;
    }

    @VisibleForTesting
    @Nonnull StarSubQuery setUp(@Nonnull StarVarIndex varIndex, int starIdx) {
        index = varIndex;
        selectors.clear();
        var2col.clear();
        StarSubQuery star = varIndex.getStar(starIdx);
        setUp(star);
        for (Triple triple : star.getTriples()) {
            SqlSelector selector = toSelector(triple);
            if (selector instanceof AssignSqlSelector) {
                AssignSqlSelector assign = (AssignSqlSelector) selector;
                String var = assign.getSparqlVar();
                var2col.put(var, assign.getColumn());
                if (!varIndex.getStarProjection(starIdx).contains(var))
                    continue;
            }
            selectors.add(selector);
        }
        return star;
    }

    private void setUpFilters(@Nullable Set<SPARQLFilter> pendingFilters, StarSubQuery star) {
        for (SPARQLFilter filter : star.getFilters()) {
            SqlSelector s = toSelector(filter);
            if (s != null) {
                selectors.add(s);
                if (pendingFilters != null) pendingFilters.remove(filter);
            }
        }
    }

    private @Nonnull StringBuilder writeSql(@Nonnull StringBuilder b,
                                            @Nonnull StarVarIndex starVarIndex, int starIdx) {
        assert selectors.stream().flatMap(s -> s.getColumns().stream()
                        .map(Column::getTable)).collect(toSet()).contains(table)
                : "this.table is invalid";
        if (selectors.stream().allMatch(AssignSqlSelector.class::isInstance))
            return b.append(table).append(" AS star_").append(starIdx);
        b.append("(SELECT  ");
        for (String sparqlVar : starVarIndex.getStarProjection(starIdx)) {
            Column column = var2col.get(sparqlVar);
            if (column == null && !star.isCore(sparqlVar))
                assert false : "No SqlSelector for " + sparqlVar + " which should be projected";
            else if (column != null)
                b.append(column).append(" AS ").append(sparqlVar).append(", ");
        }
        for (Map.Entry<String, Column> e : starVarIndex.getIdVar2Column(starIdx).entrySet())
            b.append(e.getValue()).append(" AS ").append(e.getKey()).append(", ");
        assert b.substring(b.length()-2).equals(", ") : "empty projection list";
        b.setLength(b.length()-2);

        b.append(" FROM ").append(table).append(" WHERE     ");
        for (SqlSelector selector : selectors) {
            if (selector.hasSqlCondition())
                b.append(selector.getSqlCondition(termWriter)).append(" AND ");
        }
        assert b.substring(b.length()-4).equals("AND ") : "empty WHERE clause";
        b.setLength(b.length()-4);
        return b.append(") AS star_").append(starIdx);
    }

    @VisibleForTesting
    @Nullable SqlSelector toSelector(@Nonnull Triple triple) {
        Term o = triple.getObject();
        Column column = star.getColumn(o);
        if (column == null)
            throw new IllegalArgumentException("No AtomTag->ColumnTag annotation");
        if (o.isGround())     return new EqualsSqlSelector(column, o);
        else if (o.isVar())   return new AssignSqlSelector(column, o.asVar());
        else if (o.isBlank()) return new ExistsSqlSelector(column, o.asBlank());
        throw new UnsupportedOperationException("Cannot handle "+o.getType()+" term "+o);
    }

    @VisibleForTesting
    @Nullable StringBuilder replacingOperators(@Nonnull StringBuilder b, @Nonnull Expr expr) {
        if (expr instanceof NodeValue) {
            String sql = termWriter.apply(fromJena(((NodeValue) expr).asNode()));
            return sql != null ? b.append(sql) : null;
        } else if (expr instanceof ExprVar) {
            assert var2col.get(expr.getVarName()) != null;
            return b.append(var2col.get(expr.getVarName()));
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
            if (replacingOperators(b, function.getArg(1)) == null)
                return null;
            b.append(' ').append(sql).append(' ');
            if (replacingOperators(b, function.getArg(2)) != null)
                return b.append(')');
        } else if (function.numArgs() == 1) {
            b.append(sql).append(' ');
            if (replacingOperators(b, function.getArg(1)) != null)
                return b.append(')');
        }
        return null;
    }

    @VisibleForTesting
    @Nullable SqlSelector toSelector(@Nonnull SPARQLFilter filter) {
        if (core.isVar() && filter.getVarTerms().contains(core.asVar()))
            return null;

        StringBuilder builder = replacingOperators(new StringBuilder(), filter.getExpr());
        if (builder != null) {
            List<Column> columns = new ArrayList<>();
            List<Term> vars = new ArrayList<>();
            for (Var var : filter.getVarTerms()) {
                Column column = star.getColumn(var);
                if (column == null)
                    return null; // var will not have a value in SQL
                columns.add(column);
                vars.add(var);
            }
            return new FilterSelector(columns, vars, builder.toString());
        }
        return null;
    }
}
