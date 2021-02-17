package br.ufsc.lapesd.freqel.reason.tbox.replacements;

import br.ufsc.lapesd.freqel.model.term.Var;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Objects;

public class VarReplacement extends Replacement {
    private final @Nonnull Replacement groundReplacement;

    public VarReplacement(@Nonnull Var var, @Nonnull Replacement replacement) {
        super(replacement.getTerm(), Collections.singleton(var), replacement.getCtx());
        this.groundReplacement = replacement;
    }

    public @Nonnull Var getVar() {
        return getAlternatives().iterator().next().asVar();
    }

    public @Nonnull Replacement getGroundReplacement() {
        return groundReplacement;
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VarReplacement)) return false;
        if (!super.equals(o)) return false;
        VarReplacement that = (VarReplacement) o;
        return getGroundReplacement().equals(that.getGroundReplacement());
    }

    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), getGroundReplacement());
    }

    @Override public @Nonnull String toString() {
        return getVar() + "{"+getGroundReplacement().toString()+"}";
    }
}
