package br.ufsc.lapesd.riefederator.algebra;

import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

public interface InnerOp extends Op {
    /**
     * Create a new instance of this same node type with  the given set of child nodes and modifiers
     *
     * modifiers of this instance are not copied to the new instance. They must be
     * explicitly given as the second argument if that is intended.
     *
     * @param children the set of child {@link Op} nodes
     * @param modifiers the set of {@link Modifier}s, or null for no {@link Modifier}s
     * @return a new {@link Op} of the same class
     */
    @Nonnull Op createWith(@Nonnull List<Op> children, @Nullable Collection<Modifier> modifiers);

    /**
     * Replaces the child at the given index with the new given Op.
     *
     * @param index index of the child to replace in {@link Op#getChildren()}
     * @param replacement new child node
     * @return the old child node
     */
    @Nonnull Op setChild(int index, @Nonnull Op replacement);

    /**
     * Detaches all children from this node and return their list. After this,
     * {@link Op#getChildren()} will be empty.
     */
    @Nonnull TakenChildren takeChildren();

    /**
     * Replaces the current list of children with the given one.
     *
     * @return The old list of children, which will be detached (unless the child is
     *         also in the new list)
     */
    @Nonnull List<Op> setChildren(@Nonnull List<Op> list);

    /**
     * If possible, removes all children of this node.
     *
     * This is intended to be invoked after its children have been relocated to a new node,
     * stopping useless change notifications to this node and making it collectable by the JVM GC.
     */
    void detachChildren();

    /**
     * Adds a new child to the node. Some implementations (e.g., binary joins) may
     * throw {@link UnsupportedOperationException}.
     *
     * @param child the child to add.
     * @throws IllegalArgumentException if child is already a child
     * @throws UnsupportedOperationException if the particular {@link InnerOp} disallows
     *                                       variable-sized children lists (e.g., binary joins)
     */
    void addChild(@Nonnull Op child);
}
