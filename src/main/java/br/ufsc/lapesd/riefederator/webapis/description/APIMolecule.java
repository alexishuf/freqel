package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.query.Cardinality;
import br.ufsc.lapesd.riefederator.webapis.requests.APIRequestExecutor;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

@Immutable
public class APIMolecule  {
    private final @Nonnull Molecule molecule;
    private final @Nonnull APIRequestExecutor executor;
    private final @Nonnull ImmutableMap<String, String> atom2input;
    private final @Nonnull Cardinality cardinality;

    public APIMolecule(@Nonnull Molecule molecule, @Nonnull APIRequestExecutor executor,
                       @Nonnull Map<String, String> atom2input,
                       @Nonnull Cardinality cardinality) {
        checkArgument(atom2input.values().containsAll(executor.getRequiredInputs()),
                "There are some requiredInputs in executor which are not mapped to in atom2input");
        if (APIMolecule.class.desiredAssertionStatus()) {
            HashSet<String> set = new HashSet<>(atom2input.values());
            checkArgument(set.size() == atom2input.values().size(),
                    "There are some inputs mapped to more than once in atom2input");
        }
        this.molecule = molecule;
        this.executor = executor;
        this.atom2input = ImmutableMap.copyOf(atom2input);
        this.cardinality = cardinality;
    }

    public APIMolecule(@Nonnull Molecule molecule, @Nonnull APIRequestExecutor executor,
                       @Nonnull Map<String, String> atom2input) {
        this(molecule, executor, atom2input, Cardinality.UNSUPPORTED);
    }

    public @Nonnull Molecule getMolecule() {
        return molecule;
    }

    public @Nonnull APIRequestExecutor getExecutor() {
        return executor;
    }

    public @Nonnull ImmutableMap<String, String> getAtom2input() {
        return atom2input;
    }

    public @Nonnull Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public @Nonnull String toString() {
        return format("APIMolecule(%s, |%s|, %s, %s)", getMolecule(), getCardinality(),
                                                       getAtom2input(), getExecutor());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof APIMolecule)) return false;
        APIMolecule that = (APIMolecule) o;
        return getMolecule().equals(that.getMolecule()) &&
                getExecutor().equals(that.getExecutor()) &&
                getAtom2input().equals(that.getAtom2input());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMolecule(), getExecutor(), getAtom2input());
    }
}
