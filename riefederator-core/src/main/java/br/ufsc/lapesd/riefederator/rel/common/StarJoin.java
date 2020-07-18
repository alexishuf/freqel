package br.ufsc.lapesd.riefederator.rel.common;

import javax.annotation.Nonnull;
import java.util.Objects;

public class StarJoin {
    private final int starIdx1, starIdx2;
    private @Nonnull
    final  String var1;
    private @Nonnull final String var2;
    private @Nonnull final String sparqlVar;
    private final boolean direct;

    public StarJoin(int starIdx1, int starIdx2, @Nonnull String var1, @Nonnull String var2,
                    @Nonnull String sparqlVar, boolean direct) {
        this.starIdx1 = starIdx1;
        this.starIdx2 = starIdx2;
        this.var1 = var1;
        this.var2 = var2;
        this.sparqlVar = sparqlVar;
        this.direct = direct;
    }

    public boolean contains(int starIdx, @Nonnull String var) {
        return (starIdx1 == starIdx && var1.equals(var))
                || (starIdx2 == starIdx && var2.equals(var));
    }

    public @Nonnull String getSparqlVar() {
        return sparqlVar;
    }

    public boolean isDirect() {
        return direct;
    }

    public int getStarIdx1() {
        return starIdx1;
    }
    public int getStarIdx2() {
        return starIdx2;
    }

    public int getOtherStarIdx(int starIdx) {
        if (starIdx1 == starIdx) return starIdx2;
        else if (starIdx2 ==  starIdx) return starIdx1;
        else throw new IllegalArgumentException("No starIdx="+starIdx+" in join "+this);
    }

    public @Nonnull String getVar1() {
        return var1;
    }
    public @Nonnull String getVar2() {
        return var2;
    }

    public @Nonnull String getVar(int starIdx) {
        if (starIdx1 == starIdx)
            return var1;
        else if (starIdx2 == starIdx)
            return var2;
        throw new IllegalArgumentException("No starIdx="+starIdx+" in join "+this);
    }

    public @Nonnull String getOtherVar(int starIdx) {
        if (starIdx1 == starIdx)
            return var2;
        else if (starIdx2 == starIdx)
            return var1;
        throw new IllegalArgumentException("No starIdx="+starIdx+" in join "+this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StarJoin)) return false;
        StarJoin join = (StarJoin) o;
        return getStarIdx1() == join.getStarIdx1() &&
                getStarIdx2() == join.getStarIdx2() &&
                getVar1().equals(join.getVar1()) &&
                getVar2().equals(join.getVar2());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStarIdx1(), getStarIdx2(), getVar1(), getVar2());
    }

    @Override
    public @Nonnull String toString() {
        return String.format("star_%d.%s = star_%d.%s", starIdx1, var1, starIdx2, var2);
    }
}
