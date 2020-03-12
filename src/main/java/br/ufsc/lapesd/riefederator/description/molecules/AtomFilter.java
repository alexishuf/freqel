package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.Var;
import br.ufsc.lapesd.riefederator.query.modifiers.SPARQLFilter;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.federation.tree.TreeUtils.setMinus;
import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toSet;

@Immutable
public class AtomFilter implements MoleculeElement {
    private final @Nonnull String name;
    private final @Nonnull SPARQLFilter filter;
    private final @Nonnull ImmutableBiMap<AtomWithRole, String> atom2var;

    /* --- --- --- Constructor & Builder --- --- -- */

    public AtomFilter(@Nonnull SPARQLFilter filter,
                      @Nonnull ImmutableBiMap<AtomWithRole, String> atom2var,
                      @Nonnull String name) {
        this.filter = filter;
        this.atom2var = atom2var;
        this.name = name;
    }
    public AtomFilter(@Nonnull SPARQLFilter filter,
                      @Nonnull ImmutableBiMap<AtomWithRole, String> atom2var) {
        this(filter, atom2var, generateName(filter, atom2var));
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

    public static @Nonnull WithBuilder with(@Nonnull SPARQLFilter filter) {
        return new WithBuilder(filter);
    }

    public static @Nonnull Builder builder(@Nonnull String filter) {
        return new Builder(filter);
    }

    public static class WithBuilder {
        private final @Nonnull SPARQLFilter filter;
        private @Nullable String name;
        private final @Nonnull ImmutableBiMap.Builder<AtomWithRole, String> atom2varBuilder;

        public WithBuilder(@Nonnull SPARQLFilter filter) {
            this.filter = filter;
            this.atom2varBuilder = ImmutableBiMap.builder();
        }

        public @Nonnull WithBuilder map(@Nonnull AtomWithRole atom, @Nonnull String var) {
            checkState(filter.getVars().contains(var),
                                     "Var "+var+" not in filter "+filter);
            atom2varBuilder.put(atom, var);
            return this;
        }
        public @Nonnull WithBuilder map(@Nonnull Atom atom, @Nonnull AtomRole role,
                                        @Nonnull String var) {
            return map(role.wrap(atom), var);
        }
        public @Nonnull WithBuilder map(@Nonnull String atomName, @Nonnull AtomRole role,
                                        @Nonnull String var) {
            return map(role.wrap(atomName), var);
        }

        public @Nonnull WithBuilder name(@Nonnull String name) {
            this.name = name;
            return this;
        }

        public @Nonnull AtomFilter build() {
            return name == null ? new AtomFilter(filter, atom2varBuilder.build())
                                : new AtomFilter(filter, atom2varBuilder.build(), name);
        }
    }

    public static class Builder extends SPARQLFilter.Builder {
        private final @Nonnull ImmutableBiMap.Builder<AtomWithRole, String> atom2varBuilder;
        private @Nullable String name;

        public Builder(@Nonnull String filter) {
            super(filter);
            this.atom2varBuilder = ImmutableBiMap.builder();
        }

        public @Nonnull Builder name(@Nonnull String name) {
            this.name = name;
            return this;
        }

        @Override
        public @Nonnull Builder map(@Nonnull String var, @Nonnull Term term) {
            super.map(var, term);
            return this;
        }
        @Override
        public @Nonnull Builder map(@Nonnull Var var) {
            return map(var.getName(), var);
        }

        @Override
        public @Nonnull Builder setRequired(boolean required) {
            super.setRequired(required);
            return this;
        }

        @Override
        public @Nonnull Builder advise() {
            super.advise();
            return this;
        }

        public @Nonnull Builder map(@Nonnull AtomWithRole atom, @Nonnull String var) {
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

        public @Nonnull
        AtomFilter buildFilter() {
            SPARQLFilter filter = super.build();
            ImmutableBiMap<AtomWithRole, String> atom2var = atom2varBuilder.build();
            checkState(filter.getVars().containsAll(atom2var.values()),
                       "Some vars to wich atoms map are not present in the SPARQLFilter: " +
                       setMinus(atom2var.keySet().stream().map(AtomWithRole::getAtomName)
                                                 .collect(toSet()),
                                filter.getVars())
            );
            return name == null ? new AtomFilter(filter, atom2var)
                                : new AtomFilter(filter, atom2var, name);
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
        for (Map.Entry<String, AtomWithRole> e : var2Atom.entrySet())
            b.append('?').append(e.getKey()).append('=').append(e.getValue()).append(", ");
        if (!var2Atom.isEmpty())
            b.setLength(b.length()-2);
        return b.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AtomFilter)) return false;
        AtomFilter filter1 = (AtomFilter) o;
        return getSPARQLFilter().equals(filter1.getSPARQLFilter()) &&
                atom2var.equals(filter1.atom2var);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSPARQLFilter(), atom2var);
    }
}
