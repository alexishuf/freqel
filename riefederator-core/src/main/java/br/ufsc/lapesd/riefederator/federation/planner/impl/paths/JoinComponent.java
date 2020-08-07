package br.ufsc.lapesd.riefederator.federation.planner.impl.paths;

import br.ufsc.lapesd.riefederator.algebra.Op;
import br.ufsc.lapesd.riefederator.util.ImmutableIndexedSubset;
import br.ufsc.lapesd.riefederator.util.IndexedSet;
import com.google.errorprone.annotations.Immutable;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

@Immutable
public class JoinComponent {
    @SuppressWarnings("Immutable")
    private final @Nonnull ImmutableIndexedSubset<Op> nodes;
    private @LazyInit int hash = 0;

    @SuppressWarnings("ReferenceEquality")
    public JoinComponent(@Nonnull IndexedSet<Op> allNodes,
                         @Nonnull Collection<Op> component) {
        checkArgument(!component.isEmpty(), "Empty component not allowed");
        checkArgument(allNodes.containsAll(component),
                "There nodes of component ∉ allNodes");
        if (component instanceof ImmutableIndexedSubset
                && ((ImmutableIndexedSubset<Op>) component).getParent() == allNodes) {
            nodes = (ImmutableIndexedSubset<Op>) component; //avoid copy
        } else {
            nodes = allNodes.immutableSubset(component);
        }
    }

    public JoinComponent(@Nonnull IndexedSet<Op> allNodes,
                         @Nonnull Op... nodes) {
        this(allNodes, Arrays.asList(nodes));
    }

    public JoinComponent(@Nonnull JoinGraph graph,
                         @Nonnull Collection<Op> component) {
        this(graph.getNodes(), component);
    }

    public JoinComponent(@Nonnull IndexedSet<Op> allNodes,
                         @Nonnull Op node) {
        checkArgument(allNodes.contains(node), "node ∉ allNodes");
        nodes = allNodes.immutableSubset(node);
    }

    public boolean hasJoins() {
        return nodes.size() > 1;
    }

    public boolean isWhole() {
        return nodes.size() == 1;
    }

    public @Nonnull Op getWhole() {
        return nodes.iterator().next();
    }

    public @Nonnull ImmutableIndexedSubset<Op> getNodes() {
        return nodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinComponent)) return false;
        JoinComponent other = (JoinComponent) o;
        return hashCode() == other.hashCode() &&
                Objects.equals(nodes, other.nodes);
    }

    @Override
    public int hashCode() {
        if (hash == 0) {
            int[] hashes = new int[nodes.size()];
            int idx = 0;
            for (Op node : nodes) hashes[idx++] = node.hashCode();
            Arrays.sort(hashes);
            HashCodeBuilder builder = new HashCodeBuilder();
            for (int hash : hashes)
                builder.append(hash);
            hash = builder.toHashCode();
        }
        return hash;
    }

    @Override
    public String toString() {
        assert !getNodes().isEmpty();
        StringBuilder b = new StringBuilder();
        b.append("JoinComponent{\n");
        String indent = "  ";
        for (Op node : getNodes())
            node.prettyPrint(b, indent).append('\n');
        b.append('}');
        return b.toString();
    }
}
