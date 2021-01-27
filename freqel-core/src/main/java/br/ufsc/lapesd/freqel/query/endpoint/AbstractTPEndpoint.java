package br.ufsc.lapesd.freqel.query.endpoint;

import br.ufsc.lapesd.freqel.query.CQuery;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.*;

import static java.util.Collections.emptySet;

public abstract class AbstractTPEndpoint implements TPEndpoint {
    private @Nullable WeakHashMap<TPEndpoint, Object> alternatives = null;
    private @Nonnull SoftReference<Set<TPEndpoint>> alternativesClosure
            = new SoftReference<>(null);

    @Override
    public synchronized  @Nonnull Set<TPEndpoint> getAlternatives() {
        return alternatives == null ? emptySet() : alternatives.keySet();
    }

    @Override
    public synchronized  @Nonnull Set<TPEndpoint> getAlternativesClosure() {
        Set<TPEndpoint> visited = alternativesClosure.get();
        if (visited == null) {
            visited = new HashSet<>();
            ArrayDeque<TPEndpoint> stack = new ArrayDeque<>();
            stack.push(this);
            while (!stack.isEmpty()) {
                TPEndpoint ep = stack.pop();
                if (!visited.add(ep)) continue;
                ep.getAlternatives().forEach(stack::push);
            }
            alternativesClosure = new SoftReference<>(visited);
        }
        return visited;
    }

    @Override
    public synchronized boolean isAlternative(@Nonnull TPEndpoint other) {
        return getAlternativesClosure().contains(other)
                || other.getAlternativesClosure().contains(this);
    }

    @Override
    public synchronized void addAlternatives(Collection<? extends TPEndpoint> alternatives) {
        if (!getAlternatives().containsAll(alternatives)) {
            WeakHashMap<TPEndpoint, Object> copy;
            if (this.alternatives == null)
                copy = new WeakHashMap<>();
            else
                copy = new WeakHashMap<>(this.alternatives);
            for (TPEndpoint alternative : alternatives) {
                if (alternative != null) copy.put(alternative, null);
            }
            this.alternatives = copy;
            this.alternativesClosure = new SoftReference<>(null);
        }
    }

    @Override public boolean requiresBindWithOverride() {
        return false;
    }

    @Override public boolean ignoresAtoms() {
        return false;
    }

    @Override public double alternativePenalty(@NotNull CQuery query) {
        return 0;
    }
}
