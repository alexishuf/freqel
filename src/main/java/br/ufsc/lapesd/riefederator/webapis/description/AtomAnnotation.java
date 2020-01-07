package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.TripleAnnotation;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class AtomAnnotation implements TermAnnotation, TripleAnnotation {
    private final  @Nonnull Atom atom;
    private final boolean input, required;

    public AtomAnnotation(@Nonnull Atom atom, boolean input, boolean required) {
        Preconditions.checkArgument(!required || input, "if required, it MUST be a input");
        this.atom = atom;
        this.input = input;
        this.required = required;
    }

    public AtomAnnotation(@Nonnull Atom atom) {
        this(atom, false, false);
    }

    public static @Nonnull AtomAnnotation asOptional(@Nonnull Atom atom) {
        return new AtomAnnotation(atom, true, false);
    }
    public static @Nonnull AtomAnnotation asRequired(@Nonnull Atom atom) {
        return new AtomAnnotation(atom, true, true);
    }

    public @Nonnull Atom getAtom() {
        return atom;
    }

    public @Nonnull String getAtomName() {
        return getAtom().getName();
    }

    public boolean isInput() {
        return input;
    }

    public boolean isRequired() {
        return required;
    }

    @Override
    public String toString() {
        if (isInput() && isRequired())
            return String.format("REQUIRED(%s)", getAtomName());
        else if (isInput() && !isRequired())
            return String.format("OPTIONAL(%s)", getAtomName());
        else
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
        return Objects.hash(getAtom(), isInput(), isRequired());
    }
}
