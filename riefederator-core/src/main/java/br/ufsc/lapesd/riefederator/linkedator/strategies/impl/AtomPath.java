package br.ufsc.lapesd.riefederator.linkedator.strategies.impl;

import br.ufsc.lapesd.riefederator.description.molecules.Molecule;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.SimplePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

public class AtomPath extends AbstractList<String> {
    private final Logger logger = LoggerFactory.getLogger(AtomPath.class);
    private final @Nonnull List<String> list;

    public static final AtomPath EMPTY = new AtomPath(Collections.emptyList());

    public AtomPath(@Nonnull Collection<String> coll) {
        this.list = coll instanceof List ? (List<String>) coll : new ArrayList<>(coll);
    }

    public AtomPath(@Nonnull Stream<String> stream) {
        this(stream.collect(Collectors.toList()));
    }

    /**
     * {@link SimplePath} with all predicates in the {@link AtomPath}.
     *
     * @param mol reference molecule (where the atoms are members)
     * @return SimplePath or null if the {@link AtomPath} is ambiguous
     *         (between two adjacent Atoms there is more than one predicate).
     *
     */
    public @Nullable SimplePath toSimplePath(Molecule mol) {
        checkArgument(list.stream().allMatch(a -> mol.getAtom(a) != null),
                      "Some atoms in this path do not belong to the given Molecule "+mol);
        if (list.size() < 2)
            return SimplePath.EMPTY;
        SimplePath.Builder builder = SimplePath.builder();
        Molecule.Index index = mol.getIndex();
        Iterator<String> it = list.iterator();
        String previous = it.next();
        String next = previous;
        while (it.hasNext()) {
            previous = next;
            next = it.next();
            Set<Molecule.Triple> set = index.get(previous, null, next);
            if (set.size() > 1) {
                return SimplePath.EMPTY; //ambiguous
            } else if (set.isEmpty()) {
                set = index.get(next, null, previous);
                if (set.size() > 1) {
                    return SimplePath.EMPTY; //ambiguous
                } else if (set.isEmpty()) {
                    logger.warn("No edge between atoms {} and {}! The AtomPath is invalid. " +
                            "Returning SimplePath.EMPTY", previous, next);
                    return SimplePath.EMPTY;
                } else {
                    builder.from(set.iterator().next().getEdge());
                }
            } else {
                Term predicate = set.iterator().next().getEdge();
                if (!index.get(next, null, previous).isEmpty())
                    return SimplePath.EMPTY; //ambiguous
                builder.to(predicate);
            }
        }
        return builder.build();
    }

    /**
     * Get a {@link AtomPath} sub-path starting at the given atom.
     * @param atom new starting atom
     * @return sub-path or null if atom is not in the path
     */
    public @Nullable AtomPath startingAt(@Nonnull String atom) {
        int idx = list.indexOf(atom);
        if (idx < 0) return null; //no in list
        return new AtomPath(list.subList(idx, list.size()));
    }

    @Override
    public String get(int index) {
        return list.get(index);
    }

    @Override
    public int size() {
        return list.size();
    }

    /* --- --- enforce immutability --- --- */

    @Override
    public boolean add(String s) {
        throw new UnsupportedOperationException("AtomPath is immutable");
    }

    @Override
    public String set(int index, String element) {
        throw new UnsupportedOperationException("AtomPath is immutable");
    }

    @Override
    public void add(int index, String element) {
        throw new UnsupportedOperationException("AtomPath is immutable");
    }

    @Override
    public String remove(int index) {
        throw new UnsupportedOperationException("AtomPath is immutable");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("AtomPath is immutable");
    }

    @Override
    protected void removeRange(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("AtomPath is immutable");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("AtomPath is immutable");
    }

    @Override
    public boolean addAll(@Nonnull Collection<? extends String> c) {
        throw new UnsupportedOperationException("AtomPath is immutable");
    }

    @Override
    public boolean removeAll(@Nonnull Collection<?> c) {
        throw new UnsupportedOperationException("AtomPath is immutable");
    }

    @Override
    public boolean retainAll(@Nonnull Collection<?> c) {
        throw new UnsupportedOperationException("AtomPath is immutable");
    }
}
