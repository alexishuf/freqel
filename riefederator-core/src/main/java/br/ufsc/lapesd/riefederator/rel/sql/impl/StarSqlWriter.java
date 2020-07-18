package br.ufsc.lapesd.riefederator.rel.sql.impl;

import br.ufsc.lapesd.riefederator.rel.common.Selector;
import br.ufsc.lapesd.riefederator.rel.common.StarSubQuery;
import br.ufsc.lapesd.riefederator.rel.common.StarVarIndex;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

public class StarSqlWriter {
    public static final @Nonnull StarSqlWriter INSTANCE = new StarSqlWriter();

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
     * @param index The index that contains the star and its variable dependencies
     * @param starIdx The index of the star within varIndex
     * @return <code>b</code>
     */
    @CanIgnoreReturnValue
    public @Nonnull StringBuilder write(@Nonnull StringBuilder b, @Nonnull StarVarIndex index,
                                        int starIdx) {
        StarSubQuery star = index.getStar(starIdx);
        String table = star.findTable();
        assert table != null : "No TableTag in star. Cannot generate SQL";
        if (index.getSelectors(starIdx).isEmpty())
            return b.append(star.findTable()).append(" AS star_").append(starIdx);
        b.append("(SELECT ");
        for (String v : index.getProjection(starIdx))
            b.append(index.getColumn(v)).append(" AS ").append(v).append(", ");
        assert b.substring(b.length()-2).equals(", ") : "Empty projection list";
        b.setLength(b.length()-2);

        b.append(" FROM ").append(table).append(" WHERE     ");
        for (Selector selector : index.getSelectors(starIdx)) {
            if (selector.hasCondition())
                b.append(selector.getCondition()).append(" AND ");
        }
        assert b.substring(b.length()-4).equals("AND ") : "empty WHERE clause";
        b.setLength(b.length()-4);
        return b.append(") AS star_").append(starIdx);
    }

    /**
     * Convenience version of {@link StarSqlWriter#write(StringBuilder, StarVarIndex, int)}.
     */
    @CheckReturnValue
    public @Nonnull String write(@Nonnull StarVarIndex varIndex, int starIdx) {
        return write(new StringBuilder(), varIndex, starIdx).toString();
    }

}
