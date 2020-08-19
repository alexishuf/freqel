package br.ufsc.lapesd.riefederator.query.modifiers;

import br.ufsc.lapesd.riefederator.query.CQuery;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

public class UnsafeMergeException extends IllegalArgumentException {
    private final @Nonnull Collection<Modifier> modifiers;
    private @Nullable CQuery receiving, giving;

    public UnsafeMergeException(@Nonnull Collection<Modifier> modifiers) {
        this(modifiers, null, null);
    }

    public UnsafeMergeException(@Nonnull Collection<Modifier> modifiers, @Nullable CQuery receiving,
                                @Nullable CQuery giving) {
        this.modifiers = modifiers;
        this.receiving = receiving;
        this.giving = giving;
    }

    public @Nonnull Collection<Modifier> getModifiers() {
        return modifiers;
    }

    public @Nullable CQuery getReceiving() {
        return receiving;
    }

    public void setReceiving(@Nonnull CQuery receiving) {
        this.receiving = receiving;
    }

    public @Nullable CQuery getGiving() {
        return giving;
    }

    public void setGiving(@Nonnull CQuery giving) {
        this.giving = giving;
    }

    @Override
    public @Nonnull String getMessage() {
        if (receiving == null && giving == null)
            return "Merging "+modifiers+" is unsafe";
        else
            return "Merging "+modifiers+" into "+receiving+" is unsafe. Source query: "+giving;
    }

    @Override
    public String toString() {
        return String.format("UnsafeMergeException(%s)", getMessage());
    }
}
