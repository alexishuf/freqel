package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.query.InputAnnotation;

import javax.annotation.Nonnull;
import java.util.Objects;

public class AtomInputAnnotation extends AtomAnnotation implements InputAnnotation {
    private final boolean required;

    public AtomInputAnnotation(@Nonnull Atom atom, boolean required) {
        super(atom);
        this.required = required;
    }

    public static @Nonnull AtomInputAnnotation asOptional(@Nonnull Atom atom) {
        return new AtomInputAnnotation(atom, false);
    }
    public static @Nonnull AtomInputAnnotation asRequired(@Nonnull Atom atom) {
        return new AtomInputAnnotation(atom, true);
    }

    @Override
    public boolean isInput() {
        return true;
    }

    @Override
    public boolean isRequired() {
        return required;
    }

    @Override
    public @Nonnull String toString() {
        return String.format("%s(%s)", isRequired() ? "REQUIRED" : "OPTIONAL", getAtomName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AtomInputAnnotation)) return false;
        AtomInputAnnotation that = (AtomInputAnnotation) o;
        return isRequired() == that.isRequired();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isRequired());
    }
}
