package br.ufsc.lapesd.riefederator.rel.cql.impl;

import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.FilterOperatorRewriter;
import br.ufsc.lapesd.riefederator.rel.common.RelationalTermWriter;
import br.ufsc.lapesd.riefederator.rel.common.SelectorFactory;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.NodeValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static java.util.Arrays.asList;

public class CqlFilterOperatorRewriter extends FilterOperatorRewriter {
    private static final @Nonnull Map<String, String> sparqlOp2cqlOp;

    static {
        Map<String, String> map = new HashMap<>();
        asList("<", "<=", "=", ">=", ">").forEach(o -> map.put(o, o));
        map.put("&&", "AND");
        sparqlOp2cqlOp = map;
    }


    public CqlFilterOperatorRewriter(@Nonnull RelationalTermWriter termWriter) {
        super(sparqlOp2cqlOp, termWriter);
    }

    private static @Nullable String reverseCqlOp(@Nonnull String op) {
        switch (op) {
            case "<": return ">";
            case "<=": return ">=";
            case ">": return "<";
            case ">=": return "<=";
            case "==": return "==";
            case "!=": return "!=";
        }
        return null;
    }

    @Override
    protected @Nonnull State createState(@Nonnull StringBuilder b,
                                         @Nonnull SelectorFactory.Context ctx,
                                         @Nonnull SPARQLFilter filter) {
        return new State(b, ctx, filter);
    }

    protected class State extends FilterOperatorRewriter.State {
        private @Nullable Set<String> lowerBounded, upperBounded;
        private @Nonnull List<String> columns = new ArrayList<>(2);

        public State(@Nonnull StringBuilder b, @Nonnull SelectorFactory.Context ctx,
                     @Nonnull SPARQLFilter filter) {
            super(b, ctx, filter);
        }

        @Override public boolean visitValue(NodeValue expr) {
            boolean ok = super.visitValue(expr);
            if (ok)
                columns.add(null);
            return ok;
        }

        @Override public boolean visitVar(@Nonnull Expr expr) {
            String columnName = getColumn(expr).getColumn();
            b.append(columnName);
            columns.add(columnName);
            return true;
        }

        @Override
        public boolean visitRelOp(@Nonnull ExprFunction expr, @Nonnull String cqlOp) {
            if (expr.numArgs() == 2) {
                columns.clear();
                if (!visit(expr.getArg(1)))
                    return false;
                b.append(' ').append(cqlOp).append(' ');
                if (!visit(expr.getArg(2)))
                    return false;
                if (cqlOp.equals("AND")) {
                    columns.clear();
                    return true; // no more work
                }
                if (columns.size() != 2)
                    return false; //nested expressions (and unary operaetors) not allowed in CQL
                if ((columns.get(0) != null) == (columns.get(1) != null))
                    return false; // either both sides are constants or both are columns
                if (hasDuplicateBounds(cqlOp))
                    return false;
                columns.clear();
                return true;
            }
            return false; // unary operators not allowed in CQL
        }

        public boolean hasDuplicateBounds(@Nonnull String cqlOp) {
            // undo reverse polish comparison
            if (columns.get(0) == null) {
                columns.set(0, columns.get(1));
                columns.set(1, null);
                cqlOp = reverseCqlOp(cqlOp);
                if (cqlOp == null) {
                    assert false : "Should not have reached this code!";
                    return true; // code is wrong, but for safety abort
                }
            }

            assert columns.get(0) != null;
            assert cqlOp.length() > 0;
            if (cqlOp.charAt(0) == '>') {
                if (lowerBounded == null) lowerBounded = new HashSet<>();
                if (!lowerBounded.add(columns.get(0)))
                    return true; // two lower boundaries defined for same column
            } else if (cqlOp.charAt(0) == '<') {
                if (upperBounded == null) upperBounded = new HashSet<>();
                if (!upperBounded.add(columns.get(0)))
                    return true; // two upper boundaries defined for same column
            }
            return false; // no violation
        }
    }
}
