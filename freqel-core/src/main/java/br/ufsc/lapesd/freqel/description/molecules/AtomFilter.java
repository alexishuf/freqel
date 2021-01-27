package br.ufsc.lapesd.freqel.description.molecules;

import br.ufsc.lapesd.freqel.query.modifiers.SPARQLFilter;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

@Immutable
public class AtomFilter implements MoleculeElement {
    private final @Nonnull String name;
    private final @Nonnull SPARQLFilter filter;
    private final @Nonnull ImmutableBiMap<AtomWithRole, String> atom2var;
    private final int inputIndex;
    private final boolean required;

    /* --- --- --- Constructor & Builder --- --- -- */

    public AtomFilter(@Nonnull SPARQLFilter filter,
                      @Nonnull ImmutableBiMap<AtomWithRole, String> atom2var,
                      @Nonnull String name, int inputIndex, boolean required) {
        this.filter = filter;
        this.atom2var = atom2var;
        this.name = name;
        this.inputIndex = inputIndex;
        this.required = required;
    }

    private static @Nonnull
    String generateName(@Nonnull SPARQLFilter filter,
                        @Nonnull ImmutableBiMap<AtomWithRole, String> atom2var) {
        StringBuilder b = new StringBuilder();
        b.append(filter.toString()).append("@{");
        for (Map.Entry<AtomWithRole, String> e : atom2var.entrySet())
            b.append(e.getKey()).append('=').append(e.getValue()).append(", ");
        if (!atom2var.isEmpty())
            b.setLength(b.length()-2);
        return b.append('}').toString();
    }

    public static @Nonnull Builder builder(@Nonnull SPARQLFilter filter) {
        return new Builder(filter);
    }

    public static class Builder {
        private final @Nonnull SPARQLFilter filter;
        private @Nullable String name;
        private int inputIndex = Integer.MIN_VALUE;
        private boolean required = false;
        private final @Nonnull ImmutableBiMap.Builder<AtomWithRole, String> atom2varBuilder;

        public Builder(@Nonnull SPARQLFilter filter) {
            this.filter = filter;
            this.atom2varBuilder = ImmutableBiMap.builder();
        }

        public @Nonnull Builder map(@Nonnull AtomWithRole atom, @Nonnull String var) {
            if (!filter.getVarNames().contains(var))
                throw new IllegalArgumentException("Var "+var+" not in filter "+filter);
            atom2varBuilder.put(atom, var);
            return this;
        }
        public @Nonnull Builder map(@Nonnull Atom atom, @Nonnull AtomRole role,
                                    @Nonnull String var) {
            return map(role.wrap(atom), var);
        }
        public @Nonnull Builder map(@Nonnull String atomName, @Nonnull AtomRole role,
                                    @Nonnull String var) {
            return map(role.wrap(atomName), var);
        }

        public @Nonnull Builder name(@Nonnull String name) {
            this.name = name;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull Builder withInputIndex(int inputIndex) {
            this.inputIndex = inputIndex;
            return this;
        }

        public @CanIgnoreReturnValue @Nonnull Builder required(boolean value) {
            this.required = value;
            return this;
        }

        public @Nonnull AtomFilter build() {
            ImmutableBiMap<AtomWithRole, String> atom2var = atom2varBuilder.build();
            if (name == null) name = generateName(filter, atom2var);
            return new AtomFilter(filter, atom2var, name, inputIndex, required);
        }
    }

    /* --- --- --- Getters --- --- --- */

    @Override
    public @Nonnull String getName() {
        return name;
    }

    public @Nonnull SPARQLFilter getSPARQLFilter() {
        return filter;
    }

    public @Nonnull ImmutableMap<AtomWithRole, String> getAtom2Var() {
        return atom2var;
    }

    public @Nonnull ImmutableMap<String, AtomWithRole> getVar2Atom() {
        return atom2var.inverse();
    }

    public @Nullable String getVar(AtomWithRole atom) {
        return atom2var.get(atom);
    }

    public @Nonnull Set<String> getVars() {
        return atom2var.inverse().keySet();
    }

    public boolean hasInputIndex() {
        return inputIndex != Integer.MIN_VALUE;
    }
    public int getInputIndex() {
        return inputIndex;
    }

    public boolean isRequired() {
        return required;
    }

    public @Nonnull Set<AtomWithRole> getAtoms() {
        return atom2var.keySet();
    }

    public @Nonnull Set<String> getAtomNames() {
        return getAtoms().stream().map(AtomWithRole::getAtomName).collect(toSet());
    }

    public @Nullable Atom getAtom(String var, @Nonnull Molecule molecule) {
        AtomWithRole awr = atom2var.inverse().get(var);
        if (awr == null) return null;
        Map<String, Atom> map = molecule.getAtomMap();
        Atom atom = map.get(awr.getAtomName());
        if (atom == null) {
            throw new NoSuchElementException("Filter is associated to a non-existing atom "
                                             + awr.getAtomName() + "in Molecule " + molecule);
        }
        return atom;
    }

    /* --- --- --- Object methods --- --- --- */

    @Override
    public @Nonnull String toString() {
        StringBuilder b = new StringBuilder();
        b.append("FILTER(").append(filter.getFilterString()).append(")@{");
        ImmutableMap<String, AtomWithRole> var2Atom = getVar2Atom();
        for (Map.Entry<String, AtomWithRole> e : var2Atom.entrySet()) {
            b.append('?').append(e.getKey());
            if (inputIndex != Integer.MIN_VALUE)
                b.append('[').append(inputIndex).append(']');
            b.append('=').append(e.getValue()).append(", ");
        }
        if (!var2Atom.isEmpty())
            b.setLength(b.length()-2);
        return b.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AtomFilter)) return false;
        AtomFilter rhs = (AtomFilter) o;
        return getSPARQLFilter().equals(rhs.getSPARQLFilter())
                && atom2var.equals(rhs.atom2var)
                && inputIndex == rhs.inputIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSPARQLFilter(), atom2var, inputIndex);
    }
}
