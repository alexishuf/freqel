package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.query.InputAnnotation;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class AtomAnnotation implements TermAnnotation, InputAnnotation {
    protected final boolean input, required;
    private final  @Nonnull Atom atom;

    public AtomAnnotation(@Nonnull Atom atom, boolean input, boolean required) {
        Preconditions.checkArgument(!required || input, "If it is required, it must be a input");
        this.atom = atom;
        this.input = input;
        this.required = required;
    }

    public static @Nonnull AtomAnnotation of(@Nonnull Atom atom) {
        return new AtomAnnotation(atom, false, false);
    }

    public static @Nonnull AtomInputAnnotation asOptional(@Nonnull Atom atom) {
        return AtomInputAnnotation.asOptional(atom);
    }
    public static @Nonnull AtomInputAnnotation asRequired(@Nonnull Atom atom) {
        return AtomInputAnnotation.asRequired(atom);
    }

    public @Nonnull Atom getAtom() {
        return atom;
    }

    public @Nonnull String getAtomName() {
        return getAtom().getName();
    }

    @Override
    public boolean isInput() {
        return input;
    }

    @Override
    public boolean isRequired() {
        return required;
    }

    @Override
    public String toString() {
        if (isInput())
            return String.format("%s(%s)", isRequired() ? "REQUIRED" : "OPTIONAL", getAtomName());
        return getAtomName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AtomAnnotation)) return false;
        AtomAnnotation that = (AtomAnnotation) o;
        return isInput() == that.isInput() &&
                isRequired() == that.isRequired() &&
                getAtom().equals(that.getAtom());
    }

    @Override
    public int hashCode() {
        return Objects.hash(isInput(), isRequired(), getAtom());
    }
}
