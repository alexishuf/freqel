package br.ufsc.lapesd.freqel.webapis.description;

import br.ufsc.lapesd.freqel.description.molecules.Atom;
import br.ufsc.lapesd.freqel.query.annotations.QueryRelevantTermAnnotation;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class AtomAnnotation implements QueryRelevantTermAnnotation {
    private final @Nonnull Atom atom;

    public AtomAnnotation(@Nonnull Atom atom) {
        this.atom = atom;
    }

    public static @Nonnull AtomAnnotation of(@Nonnull Atom atom) {
        return new AtomAnnotation(atom);
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
