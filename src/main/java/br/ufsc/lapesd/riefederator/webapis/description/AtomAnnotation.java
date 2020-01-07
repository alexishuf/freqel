package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.molecules.Atom;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.TripleAnnotation;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;

@Immutable
public class AtomAnnotation implements TermAnnotation, TripleAnnotation {
    private final  @Nonnull Atom atom;

    public AtomAnnotation(@Nonnull Atom atom) {
        this.atom = atom;
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

    public boolean isInput() {
        return false;
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
        return getAtom().hashCode();
    }
}
