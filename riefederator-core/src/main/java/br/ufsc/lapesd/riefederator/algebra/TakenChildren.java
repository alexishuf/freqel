package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.algebra.util.TreeUtils;
import br.ufsc.lapesd.riefederator.model.Triple;
import com.datastax.oss.driver.shaded.guava.common.base.Function;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

public class TakenChildren extends AbstractList<Op> implements AutoCloseable {
    private final @Nonnull List<Op> children;
    private final @Nullable List<Op> debugDeep; // non-null only if asserts enabled, used on close
    private final @Nonnull InnerOp parent;
    private boolean contentChange = true;

    public TakenChildren(@Nonnull InnerOp parent, @Nonnull List<Op> children) {
        this.children = children;
        this.parent = parent;
        if (getClass().desiredAssertionStatus()) {
            debugDeep = new ArrayList<>();
            for (Op child : children)
                debugDeep.add(TreeUtils.deepCopy(child));
        } else {
            debugDeep = null;
        }
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

    @Override public Op get(int index) {
        return children.get(index);
    }

    @Override public int size() {
        return children.size();
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

    @Override public void clear() {
        children.clear();
    }

    @Override public @Nonnull Iterator<Op> iterator() {
        return children.iterator();
    }

    @Override public @Nonnull ListIterator<Op> listIterator() {
        return children.listIterator();
    }

    @Override public @Nonnull ListIterator<Op> listIterator(int index) {
        return children.listIterator(index);
    }

    @Override
    public void close() {
        if (debugDeep != null) {
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
        parent.setChildren(children);
        if (contentChange)
            parent.purgeCaches();
    }
}
