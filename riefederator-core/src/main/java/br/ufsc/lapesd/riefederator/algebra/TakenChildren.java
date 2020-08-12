package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.algebra.inner.AbstractInnerOp;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.UnaryOperator;

public class TakenChildren implements List<Op>, AutoCloseable {
    private final @Nonnull List<Op> children;
    private final @Nonnull InnerOp parent;
    private boolean change = true;

    public TakenChildren(@Nonnull InnerOp parent, @Nonnull List<Op> children) {
        this.children = children;
        this.parent = parent;
    }

    public void setHasChanges(boolean change) {
        this.change = change;
    }

    @Override public int size() {
        return children.size();
    }

    @Override public boolean isEmpty() {
        return children.isEmpty();
    }

    @Override public boolean contains(Object o) {
        return children.contains(o);
    }

    @Override public @Nonnull Iterator<Op> iterator() {
        assert children instanceof ArrayList || children instanceof LinkedList;
        return children.iterator();
    }

    @Override public @Nonnull Object[] toArray() {
        return children.toArray();
    }

    @Override public @Nonnull <T> T[] toArray(@Nonnull T[] a) {
        //noinspection SuspiciousToArrayCall
        return children.toArray(a);
    }

    @Override public boolean add(Op op) {
        return children.add(op);
    }

    @Override public boolean remove(Object o) {
        return children.remove(o);
    }

    @Override public boolean containsAll(@Nonnull Collection<?> c) {
        return children.containsAll(c);
    }

    @Override public boolean addAll(@Nonnull Collection<? extends Op> c) {
        return children.addAll(c);
    }

    @Override public boolean addAll(int index, @Nonnull Collection<? extends Op> c) {
        return children.addAll(index, c);
    }

    @Override public boolean removeAll(@Nonnull Collection<?> c) {
        return children.removeAll(c);
    }

    @Override public boolean retainAll(@Nonnull Collection<?> c) {
        return children.retainAll(c);
    }

    @Override public void replaceAll(UnaryOperator<Op> operator) {
        children.replaceAll(operator);
    }

    @Override public void sort(Comparator<? super Op> c) {
        children.sort(c);
    }

    @Override public void clear() {
        children.clear();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass") @Override
    public boolean equals(Object o) {
        return children.equals(o);
    }

    @Override public int hashCode() {
        return children.hashCode();
    }

    @Override public Op get(int index) {
        return children.get(index);
    }

    @Override public Op set(int index, Op element) {
        return children.set(index, element);
    }

    @Override public void add(int index, Op element) {
        children.add(index, element);
    }

    @Override public Op remove(int index) {
        return children.remove(index);
    }

    @Override public int indexOf(Object o) {
        return children.indexOf(o);
    }

    @Override public int lastIndexOf(Object o) {
        return children.lastIndexOf(o);
    }

    @Override public @Nonnull ListIterator<Op> listIterator() {
        return children.listIterator(0);
    }

    @Override public @Nonnull ListIterator<Op> listIterator(int index) {
        assert children instanceof ArrayList || children instanceof LinkedList;
        return children.listIterator(index);
    }

    @Override public @Nonnull List<Op> subList(int fromIndex, int toIndex) {
        assert children instanceof ArrayList || children instanceof LinkedList;
        return children.subList(fromIndex, toIndex);
    }

    @Override public Spliterator<Op> spliterator() {
        return children.spliterator();
    }

    @Override
    public void close() {
        if (parent instanceof AbstractInnerOp)
            ((AbstractInnerOp)parent).setChildren(children, change);
        else
            parent.setChildren(children);
    }
}
