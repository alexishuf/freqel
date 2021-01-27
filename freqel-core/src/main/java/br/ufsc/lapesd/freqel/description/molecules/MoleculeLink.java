package br.ufsc.lapesd.freqel.description.molecules;

import br.ufsc.lapesd.freqel.description.molecules.tags.MoleculeLinkTag;
import br.ufsc.lapesd.freqel.model.term.Term;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Objects;

@Immutable
public class MoleculeLink {
    private final @Nonnull Term edge;
    private final @Nonnull Atom atom;
    private final boolean authoritative;
    private final @Nonnull ImmutableSet<MoleculeLinkTag> tags;
    private @LazyInit int stableHash = 0;

    public MoleculeLink(@Nonnull Term edge, @Nonnull Atom atom,
                        boolean authoritative,
                        @Nonnull Collection<? extends MoleculeLinkTag> tags) {
        this.edge = edge;
        this.atom = atom;
        this.authoritative = authoritative;
        this.tags = ImmutableSet.copyOf(tags);
    }

    public @Nonnull Term getEdge() {
        return edge;
    }
    public @Nonnull Atom getAtom() {
        return atom;
    }

    public @Nonnull ImmutableSet<MoleculeLinkTag> getTags() {
        return tags;
    }

    /**
     * An authoritative link means that the set of instances found for this link in a source
     * are the universal set of existing links for the same subject.
     *
     * Not to be confused with {@link Atom}.isExclusive(). isExclusive() means that no other
     * source will have the the same subject. Thus isExclusive implies isAuthoritative for all
     * links, but the reverse does not hold in general.
     */
    public boolean isAuthoritative() {
        return authoritative;
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)", getEdge(), getAtom());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MoleculeLink)) return false;
        MoleculeLink that = (MoleculeLink) o;
        return isAuthoritative() == that.isAuthoritative() &&
                getEdge().equals(that.getEdge()) &&
                getAtom().equals(that.getAtom()) &&
                getTags().equals(that.getTags());
    }

    @Override
    public int hashCode() {
        int local = stableHash;
        if (local == 0)
            stableHash = local = Objects.hash(getEdge(),isAuthoritative(), getTags());
        return 31 * local + getAtom().hashCode();
    }
}
