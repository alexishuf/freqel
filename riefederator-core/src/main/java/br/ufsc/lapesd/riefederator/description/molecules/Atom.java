package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;


@Immutable
public class Atom implements MoleculeElement {
    private final @Nonnull String name;
    private final boolean exclusive, closed, disjoint;
    private final @Nonnull ImmutableSet<MoleculeLink> in, out;
    private final @Nonnull ImmutableSet<AtomTag> tags;
    private @LazyInit int hash = 0;

    public Atom(@Nonnull String name, boolean exclusive, boolean closed,
                boolean disjoint, @Nonnull Set<MoleculeLink> in,
                @Nonnull Set<MoleculeLink> out, @Nonnull Collection<AtomTag> tags) {
        this.name = name;
        this.exclusive = exclusive;
        this.closed = closed;
        this.disjoint = disjoint;
        this.in = ImmutableSet.copyOf(in);
        this.out = ImmutableSet.copyOf(out);
        this.tags = ImmutableSet.copyOf(tags);
    }

    /**
     * Creates an Atom without any links. Useful for representing literals.
     *
     * The atom will be non-exclusive and non-closed.
     */
    public Atom(@Nonnull String name) {
        this(name, false, false, false, emptySet(), emptySet(), emptyList());
    }

    @Override
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


    /**
     * When an {@link Atom} is closed, it can be safely assumed that no subject of that
     * atom will ever present an incoming or outgoing link that is not in the atom description.
     *
     * If an atom is both exclusive and closed, if a conjunctive query does not fully
     * match the atom, then the source can be safely discarded for that query.
     */
    public boolean isClosed() {
        return  closed;
    }

    /**
     * A <b>disjoint</b> atom is one for which no instance is also an instance of another
     * atom in the same molecule.
     *
     * Disjointness of an atom can prove nullity of some queries when matched against a
     * molecule, since some triple patterns can be proven to yield no results, a result
     * that can cascade to other triple patterns.
     */
    public boolean isDisjoint() {
        return disjoint;
    }

    public @Nonnull Set<MoleculeLink> getIn() {
        return in;
    }
    public @Nonnull Set<MoleculeLink> getOut() {
        return out;
    }

    public @Nonnull ImmutableSet<AtomTag> getTags() {
        return tags;
    }

    public @Nonnull Stream<MoleculeLink> streamLinks() {
        return Stream.concat(getIn().stream(), getOut().stream());
    }

    public @Nonnull Stream<Atom> streamNeighbors() {
        return streamLinks().map(MoleculeLink::getAtom).distinct();
    }

    /** The number of incoming and outgoing edges. */
    public int edgesCount() {
        return getIn().size() + getOut().size();
    }

    @Override
    public @Nonnull String toString() {
        return name;
    }

    public @Nonnull String dump() {
        return dump(StdPrefixDict.DEFAULT);
    }

    public @Nonnull String dump(@Nonnull PrefixDict dict) {
        return dump(new StringBuilder(8 + (in.size() + out.size())*32), 0, dict);
    }

    public @Nonnull String dump(StringBuilder b, int indent, @Nonnull PrefixDict dict) {
        String space = new String(new char[indent]).replace('\0', ' ');
        b.append(dict.shorten(getName()).toString());
        if (isExclusive() || isClosed() || isDisjoint()) {
            b.append("[");
            if (isExclusive()) b.append("exclusive,");
            if (isClosed()) b.append("closed,");
            if (isDisjoint()) b.append("disjoint,");
            b.setLength(b.length()-1); //erase last ','
            b.append("]");
        }
        b.append('\n');
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
                isClosed() == atom.isClosed() &&
                isDisjoint() == atom.isDisjoint() &&
                getIn().equals(atom.getIn()) &&
                getOut().equals(atom.getOut()) &&
                getTags().equals(atom.getTags());
    }

    @Override
    public int hashCode() {
        if (hash == 0)
            hash = Objects.hash(getName(), isExclusive(), isClosed(), isDisjoint(),
                                getIn(), getOut(), getTags());
        return hash;
    }
}
