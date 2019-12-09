package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;


@Immutable
public class Atom {
    private final @Nonnull String name;
    private final boolean exclusive;
    private final @Nonnull ImmutableSet<MoleculeLink> in, out;
    private @LazyInit int hash = 0;

    public Atom(@Nonnull String name, boolean exclusive,
                @Nonnull Set<MoleculeLink> in,
                @Nonnull Set<MoleculeLink> out) {
        this.name = name;
        this.exclusive = exclusive;
        this.in = ImmutableSet.copyOf(in);
        this.out = ImmutableSet.copyOf(out);
    }

    public @Nonnull String getName() {
        return name;
    }

    /**
     * If an {@link Atom} the subject of instance triples will be found in a single source.
     * If it is found on multiple sources, then the other occurrences will be duplicates.
     *
     * Exclusiveness implies the source has complete knowledge of the subject. No other
     * source will have additional information not present in this molecule about the subject
     */
    public boolean isExclusive() {
        return exclusive;
    }

    public @Nonnull Set<MoleculeLink> getIn() {
        return in;
    }
    public @Nonnull Set<MoleculeLink> getOut() {
        return out;
    }

    @Override
    public @Nonnull String toString() {
        return name;
    }

    public @Nonnull String dump() {
        return dump(StdPrefixDict.STANDARD);
    }

    public @Nonnull String dump(@Nonnull PrefixDict dict) {
        return dump(new StringBuilder(8 + (in.size() + out.size())*32), 0, dict);
    }

    public @Nonnull String dump(StringBuilder b, int indent, @Nonnull PrefixDict dict) {
        String space = new String(new char[indent]).replace('\0', ' ');
        b.append(dict.shorten(getName()).toString()).append('\n');
        for (MoleculeLink link : getIn()) {
            b.append(space).append("<-(").append(link.getEdge().toString(dict)).append(")-- ");
            link.getAtom().dump(b, indent+2, dict);
        }
        for (MoleculeLink link : getOut()) {
            b.append(space).append("--(").append(link.getEdge().toString(dict)).append(")-> ");
            link.getAtom().dump(b, indent+2, dict);
        }
        return b.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Atom)) return false;
        Atom atom = (Atom) o;
        return getName().equals(atom.getName()) &&
                isExclusive() == atom.isExclusive() &&
                getIn().equals(atom.getIn()) &&
                getOut().equals(atom.getOut());
    }

    @Override
    public int hashCode() {
        if (hash == 0)
            hash = Objects.hash(getName(), isExclusive(), getIn(), getOut());
        return hash;
    }
}
