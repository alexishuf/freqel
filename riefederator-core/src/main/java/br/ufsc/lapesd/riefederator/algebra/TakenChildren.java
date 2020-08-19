package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.algebra.inner.AbstractInnerOp;
import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import com.datastax.oss.driver.shaded.guava.common.base.Function;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class TakenChildren extends AbstractList<Op> implements AutoCloseable {
    private final @Nonnull List<Op> children;
    private final @Nullable List<Op> debugDeep, debugShallow;
    private final @Nonnull InnerOp parent;
    private boolean refsChange = false;
    private boolean contentChange = true;

    public TakenChildren(@Nonnull InnerOp parent, @Nonnull List<Op> children) {
        this.children = children;
        this.parent = parent;
        if (getClass().desiredAssertionStatus()) {
            debugDeep = new ArrayList<>();
            debugShallow = new ArrayList<>();
            for (Op child : children) {
                debugShallow.add(child);
                debugDeep.add(TreeUtils.deepCopy(child));
            }
        } else {
            debugDeep = null;
            debugShallow = null;
        }
    }

    @VisibleForTesting
    boolean hasRefsChange() {
        return refsChange;
    }

    @VisibleForTesting
    boolean hasContentChange() {
        return contentChange;
    }

    /**
     * Marks that while there may have been replaced child nodes, the contents of the nodes
     * in terms of variables and matched triples have not changed
     */
    public  @Nonnull TakenChildren setNoContentChange() {
        contentChange = false;
        return this;
    }

    private class Itr implements Iterator<Op> {
        private final @Nonnull Iterator<Op> delegate;
        protected  @Nullable Op lastOp;

        public Itr(@Nonnull Iterator<Op> delegate) {
            this.delegate = delegate;
        }

        @Override public boolean hasNext() { return delegate.hasNext(); }
        @Override public Op next() { return (lastOp = delegate.next()); }

        @Override public void remove() {
            delegate.remove();
            refsChange = true;
            lastOp = null;
        }

        @Override public void forEachRemaining(Consumer<? super Op> action) {
            delegate.forEachRemaining(action);
        }
    }

    private class ListItr extends Itr implements ListIterator<Op> {
        private final @Nonnull ListIterator<Op> delegate;

        public ListItr(@Nonnull ListIterator<Op> delegate) {
            super(delegate);
            this.delegate = delegate;
        }

        @Override public boolean hasPrevious() { return delegate.hasPrevious(); }
        @Override public Op previous() { return (lastOp = delegate.previous()); }
        @Override public int nextIndex() { return delegate.nextIndex(); }
        @Override public int previousIndex() { return delegate.previousIndex(); }

        @Override public void set(Op op) {
            assert lastOp != null;
            if (lastOp != op)
                refsChange = true;
            delegate.set(op);
        }

        @Override public void add(Op op) {
            delegate.add(op);
            refsChange = true;
        }
    }

    @Override public Op get(int index) {
        return children.get(index);
    }

    @Override public int size() {
        return children.size();
    }

    @Override public boolean add(Op op) {
        refsChange = true;
        return super.add(op);
    }

    @Override public Op set(int index, Op element) {
        refsChange |= children.get(index) != element;
        return children.set(index, element);
    }

    @Override public void add(int index, Op element) {
        refsChange = true;
        children.add(index, element);
    }

    @Override public Op remove(int index) {
        refsChange = true;
        return children.remove(index);
    }

    @Override public void clear() {
        refsChange = true;
        children.clear();
    }

    @Override public @Nonnull Iterator<Op> iterator() {
        return new Itr(children.iterator());
    }

    @Override public @Nonnull ListIterator<Op> listIterator() {
        return new ListItr(children.listIterator());
    }

    @Override public @Nonnull ListIterator<Op> listIterator(int index) {
        return new ListItr(children.listIterator(index));
    }

    @Override
    public void close() {
        if (debugDeep != null) {
            assert debugShallow != null;
            assert debugShallow.size() == debugDeep.size();
            boolean sameRefs = debugShallow.size() == children.size();
            for (int i = 0, size = children.size(); sameRefs && i < size; i++)
                sameRefs = debugShallow.get(i) == children.get(i);
            assert !refsChange == sameRefs : "refsChange flag is wrong!";

            if (!contentChange) {
                Set<Triple> oMT = debugDeep.stream().flatMap(o -> o.getMatchedTriples().stream())
                                                        .collect(toSet());
                Set<Triple> cMT = children.stream().flatMap(o -> o.getMatchedTriples().stream())
                                                   .collect(toSet());
                assert oMT.equals(cMT) : "!contentChange, but matched triples changed";
                List<Function<Op, Stream<String>>> fs = Arrays.asList(
                        o -> Objects.requireNonNull(o).getResultVars().stream(),
                        o -> Objects.requireNonNull(o).getPublicVars().stream(),
                        o -> Objects.requireNonNull(o).getInputVars().stream(),
                        o -> Objects.requireNonNull(o).getRequiredInputVars().stream(),
                        o -> Objects.requireNonNull(o).getOptionalInputVars().stream());
                for (Function<Op, Stream<String>> f : fs) {
                    Set<String> o = debugDeep.stream().flatMap(f).collect(toSet());
                    Set<String> c =      children.stream().flatMap(f).collect(toSet());
                    assert c.equals(o) : "!contentChange, but "+c+" != "+o;
                }
            }
        }
        if (parent instanceof AbstractInnerOp)
            ((AbstractInnerOp)parent).setChildren(children, refsChange, contentChange);
        else
            parent.setChildren(children);
    }
}
