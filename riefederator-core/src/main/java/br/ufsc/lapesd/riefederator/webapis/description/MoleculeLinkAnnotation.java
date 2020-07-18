package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.molecules.MoleculeLink;
import br.ufsc.lapesd.riefederator.query.annotations.QueryRelevantTripleAnnotation;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.Objects;

@Immutable
public class MoleculeLinkAnnotation implements QueryRelevantTripleAnnotation {
    private final @Nonnull MoleculeLink moleculeLink;
    private final boolean reversed;

    public MoleculeLinkAnnotation(@Nonnull MoleculeLink moleculeLink, boolean reversed) {
        this.moleculeLink = moleculeLink;
        this.reversed = reversed;
    }

    public @Nonnull MoleculeLink getLink() {
        return moleculeLink;
    }

    /**
     * If reversed this MoleculeLink is in the in() set of the object. Else it is in the out()
     * set of the subject.
     */
    public boolean isReversed() {
        return reversed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MoleculeLinkAnnotation)) return false;
        MoleculeLinkAnnotation that = (MoleculeLinkAnnotation) o;
        return isReversed() == that.isReversed() &&
                moleculeLink.equals(that.moleculeLink);
    }

    @Override
    public int hashCode() {
        return Objects.hash(moleculeLink, isReversed());
    }

    @Override
    public @Nonnull String toString() {
        return (reversed ? "[reversed]" : "") + moleculeLink.toString();
    }
}
