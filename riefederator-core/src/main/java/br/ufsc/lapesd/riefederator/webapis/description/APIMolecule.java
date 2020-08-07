package br.ufsc.lapesd.riefederator.webapis.description;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.webapis.requests.APIRequestExecutor;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

@Immutable
public class APIMolecule  {
    private final @Nonnull Molecule molecule;
    private final @Nonnull APIRequestExecutor executor;
    private final @Nonnull ImmutableMap<String, String> element2input;
    private final @Nonnull Cardinality cardinality;
    private final @Nullable String name;

    public APIMolecule(@Nonnull Molecule molecule, @Nonnull APIRequestExecutor executor,
                       @Nonnull Map<String, String> element2input,
                       @Nonnull Cardinality cardinality, @Nullable String name) {
        checkArgument(element2input.values().containsAll(executor.getRequiredInputs()),
                "There are some requiredInputs in executor which are not mapped to in element2input");
        if (APIMolecule.class.desiredAssertionStatus()) {
            HashSet<String> set = new HashSet<>(element2input.values());
            checkArgument(set.size() == element2input.values().size(),
                    "There are some inputs mapped to more than once in atom2input");
        }
        this.molecule = molecule;
        this.executor = executor;
        this.element2input = ImmutableMap.copyOf(element2input);
        this.cardinality = cardinality;
        this.name = name;
    }

    public APIMolecule(@Nonnull Molecule molecule, @Nonnull APIRequestExecutor executor,
                       @Nonnull Map<String, String> element2input) {
        this(molecule, executor, element2input, Cardinality.UNSUPPORTED, null);
    }

    public @Nullable String getName() {
        return name;
    }

    public @Nonnull Molecule getMolecule() {
        return molecule;
    }

    public @Nonnull APIRequestExecutor getExecutor() {
        return executor;
    }

    public @Nonnull ImmutableMap<String, String> getElement2Input() {
        return element2input;
    }

    public @Nonnull Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public @Nonnull String toString() {
        return format("APIMolecule(%s, |%s|, %s, %s)", getMolecule(), getCardinality(),
                                                       getElement2Input(), getExecutor());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof APIMolecule)) return false;
        APIMolecule that = (APIMolecule) o;
        return getMolecule().equals(that.getMolecule()) &&
                getExecutor().equals(that.getExecutor()) &&
                getElement2Input().equals(that.getElement2Input());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMolecule(), getExecutor(), getElement2Input());
    }
}
