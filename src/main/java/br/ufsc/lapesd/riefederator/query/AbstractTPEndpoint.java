package br.ufsc.lapesd.riefederator.query;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.WeakHashMap;

import static java.util.Collections.emptySet;

public abstract class AbstractTPEndpoint implements TPEndpoint {
    private @Nullable WeakHashMap<TPEndpoint, Object> alternatives = null;

    @Override
    public @Nonnull synchronized Set<TPEndpoint> getAlternatives() {
        return alternatives == null ? emptySet() : alternatives.keySet();
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
        }
    }
}
