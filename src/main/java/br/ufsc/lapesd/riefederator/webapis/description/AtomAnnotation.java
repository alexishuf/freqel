package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

@Immutable
public class AtomAnnotation implements TermAnnotation {
    private final @Nonnull Atom atom;

    public AtomAnnotation(@Nonnull Atom atom) {
        this.atom = atom;
    }

    public static @Nonnull AtomAnnotation of(@Nonnull Atom atom) {
        return new AtomAnnotation(atom);
    }

    public static @Nonnull AtomInputAnnotation asOptional(@Nonnull Atom atom,
                                                          @Nonnull String inputName) {
        return AtomInputAnnotation.asOptional(atom, inputName);
    }
    public static @Nonnull AtomInputAnnotation asOptional(@Nonnull Atom atom,
                                                          @Nonnull String inputName,
                                                          @Nullable Term overrideValue) {
        return AtomInputAnnotation.asOptional(atom, inputName, overrideValue);
    }
    public static @Nonnull AtomInputAnnotation asRequired(@Nonnull Atom atom,
                                                          @Nonnull String inputName) {
        return AtomInputAnnotation.asRequired(atom, inputName);
    }
    public static @Nonnull AtomInputAnnotation asRequired(@Nonnull Atom atom,
                                                          @Nonnull String inputName,
                                                          @Nullable Term overrideValue) {
        return AtomInputAnnotation.asRequired(atom, inputName, overrideValue);
    }

    public @Nonnull Atom getAtom() {
        return atom;
    }

    public @Nonnull String getAtomName() {
        return getAtom().getName();
    }

    @Override
    public String toString() {
        return getAtomName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AtomAnnotation)) return false;
        AtomAnnotation that = (AtomAnnotation) o;
        return getAtom().equals(that.getAtom());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getAtom());
    }
}
