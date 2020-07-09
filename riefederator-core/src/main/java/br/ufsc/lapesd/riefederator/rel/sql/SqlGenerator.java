package br.ufsc.lapesd.riefederator.rel.sql;

import br.ufsc.lapesd.riefederator.model.term.std.StdVar;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.modifiers.*;
import br.ufsc.lapesd.riefederator.rel.common.StarSubQuery;
import br.ufsc.lapesd.riefederator.rel.common.StarVarIndex;
import br.ufsc.lapesd.riefederator.rel.common.StarsHelper;
import br.ufsc.lapesd.riefederator.rel.mappings.Column;
import br.ufsc.lapesd.riefederator.rel.mappings.RelationalMapping;
import br.ufsc.lapesd.riefederator.rel.sql.impl.StarSqlWriter;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
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
    private @Nonnull SqlTermWriter termWriter = DefaultSqlTermWriter.INSTANCE;
    private boolean canDistinct = true, canLimit = true;

    public SqlGenerator(@Nonnull RelationalMapping mapping) {
        this.mapping = mapping;
    }

    @CanIgnoreReturnValue
    public @Nonnull SqlGenerator setTermWriter(@Nonnull SqlTermWriter termWriter) {
        this.termWriter = termWriter;
        return this;
    }
    public @Nonnull SqlTermWriter getTermWriter() {
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

    public @Nonnull SqlRewriting transform(@Nonnull CQuery query) {
        return transform(query, StarsHelper.getFilters(query));
    }

    public @Nonnull SqlRewriting transform(@Nonnull CQuery query,
                                           @Nonnull IndexedSet<SPARQLFilter> filters) {
        Preconditions.checkArgument(query.isJoinConnected());
        boolean distinct = canDistinct() &&
                ModifierUtils.getFirst(Distinct.class, query.getModifiers()) != null;
        Limit limit = null;
        if (canLimit()) {
            if (ModifierUtils.getFirst(Ask.class, query.getModifiers()) != null)
                limit = Limit.required(1);
            else
                limit = ModifierUtils.getFirst(Limit.class, query.getModifiers());
        }

        IndexedSubset<SPARQLFilter> pendingFilters = filters.fullSubset();
        List<StarSubQuery> stars = StarVarIndex.orderJoinable(StarsHelper.findStars(query));
        StarVarIndex vars = new StarVarIndex(query, stars, mapping);
        StarSqlWriter starWriter = new StarSqlWriter(termWriter);

        List<String> starSqls = new ArrayList<>(stars.size());
        BitSet simpleStars = new BitSet(stars.size());
        for (int i = 0, size = stars.size(); i < size; i++) {
            starSqls.add(starWriter.write(vars, i, pendingFilters));
            if (!SUB_QRY_RX.matcher(starSqls.get(i)).find())
                simpleStars.set(i);
        }
        StringBuilder b = new StringBuilder();
        b.append("SELECT  ").append(distinct ? "DISTINCT  " : "");
        Set<String> resultVars = writeProjection(b, vars, simpleStars);
        b.append(" FROM\n  ");

        b.append(starSqls.get(0)).append('\n');
        for (int i = 1, size = stars.size(); i < size; i++) {
            b.append("  INNER JOIN ");
            b.append(starSqls.get(i));
            writeJoin(b.append("\n    ON "), vars, i, simpleStars).append('\n');
        }
        if (limit != null) // SQL standard syntax, there are non-standard alternatives
            b.append("FETCH NEXT ").append(limit.getValue()).append(" ROWS ONLY ");
        String sql = b.append(';').toString();
        IndexedSubset<SPARQLFilter> doneFilters = filters.fullSubset();
        doneFilters.removeAll(pendingFilters);
        return new SqlRewriting(sql, resultVars, distinct, limit != null, query,
                                pendingFilters, doneFilters, vars);
    }

    @VisibleForTesting
    @Nonnull StringBuilder writeJoin(@Nonnull StringBuilder b,
                                     @Nonnull StarVarIndex varIndex, int i,
                                     @Nonnull BitSet simpleStars) {
        StarSubQuery myStar = varIndex.getStar(i);
        IndexedSubset<String> mine = varIndex.getStarProjection(i);
        for (String v : mine) {
            int prev = varIndex.firstStar(v);
            if (prev >= i) continue;
            Column prevColumn = varIndex.getColumn(v);
            Column myColumn = myStar.getColumn(new StdVar(v));
            if (prevColumn == null || myColumn == null) {
                assert false : "Could not get column of "+v+" in stars "+prev+" and "+i;
                continue;
            }
            String prevName = simpleStars.get(prev) ? prevColumn.getColumn(): v;
            String myName = simpleStars.get(i) ? myColumn.getColumn(): v;
            b.append("(star_").append(i).append('.').append(myName).append(" = ")
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
        for (String name : vars.getOuterProjection()) {
            int starIdx = vars.firstStar(name);
            assert starIdx >= 0;
            Column column = vars.getColumn(name);
            if (column == null)
                continue;
            set.add(name);
            String nameInStar = simpleStars.get(starIdx) ? column.getColumn() : name;
            b.append("star_").append(starIdx).append('.').append(nameInStar)
                             .append(" AS ").append(name).append(", ");
        }
        if (!set.isEmpty())
            b.setLength(b.length()-2); //remove last ", "
        else
            b.append('1'); // ASK query
        return set;
    }
}
