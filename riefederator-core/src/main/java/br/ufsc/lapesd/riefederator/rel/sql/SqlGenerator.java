package br.ufsc.lapesd.riefederator.rel.sql;

import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.Distinct;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifierUtils;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import br.ufsc.lapesd.riefederator.rel.common.RelationalTermWriter;
import br.ufsc.lapesd.riefederator.rel.common.StarJoin;
import br.ufsc.lapesd.riefederator.rel.common.StarVarIndex;
import br.ufsc.lapesd.riefederator.rel.mappings.RelationalMapping;
import br.ufsc.lapesd.riefederator.rel.sql.impl.DefaultSqlTermWriter;
import br.ufsc.lapesd.riefederator.rel.sql.impl.SqlSelectorFactory;
import br.ufsc.lapesd.riefederator.rel.sql.impl.StarSqlWriter;
import br.ufsc.lapesd.riefederator.util.IndexedSubset;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.regex.Pattern;

public class SqlGenerator {
    private static final Pattern SUB_QRY_RX = Pattern.compile("^\\s*\\(?\\s*SELECT");

    private final @Nonnull RelationalMapping mapping;
    private @Nonnull RelationalTermWriter termWriter = DefaultSqlTermWriter.INSTANCE;
    private boolean canDistinct = true, canLimit = true;
    private boolean exposeJoinVars = false;

    public SqlGenerator(@Nonnull RelationalMapping mapping) {
        this.mapping = mapping;
    }

    @CanIgnoreReturnValue
    public @Nonnull SqlGenerator setTermWriter(@Nonnull RelationalTermWriter termWriter) {
        this.termWriter = termWriter;
        return this;
    }
    public @Nonnull
    RelationalTermWriter getTermWriter() {
        return termWriter;
    }

    @CanIgnoreReturnValue
    public @Nonnull SqlGenerator setCanDistinct(boolean canDistinct) {
        this.canDistinct = canDistinct;
        return this;
    }
    public boolean canDistinct() {
        return canDistinct;
    }

    @CanIgnoreReturnValue
    public @Nonnull SqlGenerator setCanLimit(boolean value) {
        canLimit = value;
        return this;
    }
    public boolean canLimit() {
        return canLimit;
    }

    @CanIgnoreReturnValue
    public @Nonnull SqlGenerator setExposeJoinVars(boolean exposeJoinVars) {
        this.exposeJoinVars = exposeJoinVars;
        return this;
    }

    public boolean getExposeJoinVars() {
        return exposeJoinVars;
    }

    public @Nonnull
    RelationalRewriting transform(@Nonnull CQuery query) {
        Preconditions.checkArgument(query.attr().isJoinConnected());
        boolean distinct = canDistinct() &&
                ModifierUtils.getFirst(Distinct.class, query.getModifiers()) != null;
        int limit = 0;
        if (canLimit()) {
            if (query.attr().isAsk()) limit = 1;
            else                      limit = query.attr().limit();
        }
        SqlSelectorFactory selectorFactory = new SqlSelectorFactory(termWriter);
        StarVarIndex vars = new StarVarIndex(query, selectorFactory);

        List<String> starSqls = new ArrayList<>(vars.getStarCount());
        BitSet simpleStars = new BitSet(vars.getStarCount());
        for (int i = 0, size = vars.getStarCount(); i < size; i++) {
            starSqls.add(StarSqlWriter.INSTANCE.write(vars, i));
            if (!SUB_QRY_RX.matcher(starSqls.get(i)).find())
                simpleStars.set(i);
        }
        StringBuilder b = new StringBuilder();
        b.append("SELECT  ").append(distinct ? "DISTINCT  " : "");
        Set<String> resultVars = writeProjection(b, vars, simpleStars);
        b.append(" FROM\n  ");

        b.append(starSqls.get(0)).append('\n');
        for (int i = 1, size = vars.getStarCount(); i < size; i++) {
            b.append("  INNER JOIN ");
            b.append(starSqls.get(i));
            writeJoin(b.append("\n    ON "), vars, i, simpleStars).append('\n');
        }
        if (limit > 0) // SQL standard syntax, there are non-standard alternatives
            b.append("FETCH NEXT ").append(limit).append(" ROWS ONLY ");
        String sql = b.append(';').toString();



        IndexedSubset<SPARQLFilter> pendingFilters, doneFilters;
        pendingFilters = vars.getAllFilters().subset(vars.getCrossStarFilters());
        for (int i = 0, size = vars.getStarCount(); i < size; i++)
            pendingFilters.addAll(vars.getPendingFilters(i));

        doneFilters = vars.getAllFilters().fullSubset();
        doneFilters.removeAll(pendingFilters);
        return new RelationalRewriting(sql, resultVars, distinct, limit > 0,
                                       pendingFilters, doneFilters, vars);
    }

    @VisibleForTesting
    @Nonnull StringBuilder writeJoin(@Nonnull StringBuilder b,
                                     @Nonnull StarVarIndex varIndex, int curr,
                                     @Nonnull BitSet simpleStars) {
        for (StarJoin join : varIndex.getJoins(curr)) {
            String myName = join.getVar(curr);
            int prev = join.getOtherStarIdx(curr);
            assert prev < curr;
            String prevName = join.getVar(prev);
            if (simpleStars.get(curr)) myName = varIndex.getColumn(myName).getColumn();
            if (simpleStars.get(prev)) prevName = varIndex.getColumn(prevName).getColumn();
            b.append("(star_").append(curr).append('.').append(myName).append(" = ")
                    .append("star_").append(prev).append('.').append(prevName)
                    .append(") AND ");
        }
        assert b.substring(b.length()-4).equals("AND ");
        b.setLength(b.length()-4);
        return b;
    }

    private @Nonnull Set<String> writeProjection(@Nonnull StringBuilder b,
                                                 @Nonnull StarVarIndex vars,
                                                 @Nonnull BitSet simpleStars) {
        Set<String> set = new HashSet<>();
        for (String v : vars.getOuterProjection()) {
            int starIdx = vars.getFirstStar(v);
            assert starIdx >= 0;
            set.add(v);
            String nameInStar = simpleStars.get(starIdx) ? vars.getColumn(v).getColumn() : v;
            b.append("star_").append(starIdx).append('.').append(nameInStar)
                             .append(" AS ").append(v).append(", ");
        }
        if (!set.isEmpty())
            b.setLength(b.length()-2); //remove last ", "
        else
            b.append('1'); // ASK query
        return set;
    }
}
