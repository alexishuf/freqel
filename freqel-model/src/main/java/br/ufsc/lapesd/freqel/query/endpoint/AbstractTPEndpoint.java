package br.ufsc.lapesd.freqel.query.endpoint;

import br.ufsc.lapesd.freqel.description.AskDescription;
import br.ufsc.lapesd.freqel.description.Description;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.results.Results;
import com.google.errorprone.annotations.concurrent.LazyInit;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.SoftReference;
import java.util.*;
import java.util.function.Function;

import static java.util.Collections.emptySet;

public abstract class AbstractTPEndpoint implements TPEndpoint {
    private @Nullable WeakHashMap<TPEndpoint, Object> alternatives = null;
    private @Nonnull SoftReference<Set<TPEndpoint>> alternativesClosure
            = new SoftReference<>(null);
    protected @Nonnull Function<TPEndpoint, ? extends Description> descriptionFactory;
    protected @LazyInit @Nullable Description description = null;

    public AbstractTPEndpoint() {
        this(AskDescription::new);
    }

    public AbstractTPEndpoint(@Nonnull Function<TPEndpoint, ? extends Description> factory) {
        this.descriptionFactory = factory;
    }

    @Override public @Nonnull Results query(@Nonnull Triple query) {
        return query(CQuery.from(query));
    }

    public @Nonnull TPEndpoint setDescription(@Nonnull Description description) {
        this.description = description;
        this.descriptionFactory = e -> description;
        return this;
    }

    @Override public @Nonnull Description getDescription() {
        if (description == null)
            description = descriptionFactory.apply(this);
        return description;
    }

    @Override public boolean isWebAPILike() {
        return false;
    }

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
