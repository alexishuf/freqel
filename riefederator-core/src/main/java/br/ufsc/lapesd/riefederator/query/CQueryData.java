package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.annotations.QueryAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import br.ufsc.lapesd.riefederator.query.modifiers.ModifiersSet;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CQueryData {
    public final @Nonnull AtomicInteger references = new AtomicInteger(0);
    public @Nonnull List<Triple> list;
    public final @Nonnull ModifiersSet modifiers, modifiersView;

    public @Nonnull SetMultimap<Term, TermAnnotation> termAnnotations;
    public @Nonnull Set<QueryAnnotation> queryAnnotations;
    public @Nonnull SetMultimap<Triple, TripleAnnotation> tripleAnnotations;
    private @Nullable AtomicInteger nextHiddenId;
    public final @Nonnull CQueryCache cache;

    private class ModifierListener extends ModifiersSet.Listener {
        @Override
        public void added(@Nonnull Modifier modifier) {
            cache.notifyModifierChange(modifier.getClass());
        }

        @Override
        public void removed(@Nonnull Modifier modifier) {
            cache.notifyModifierChange(modifier.getClass());
        }
    }

    private @Nonnull ModifiersSet createModifierSet(@Nullable Collection<Modifier> collection) {
        ModifiersSet set = new ModifiersSet(collection);
        set.addListener(new ModifierListener());
        return set;
    }

    public CQueryData(@Nonnull List<Triple> list) {
        this(list, null, null, null, null);
    }

    public CQueryData(@Nonnull List<Triple> list, @Nullable Set<Modifier> modifiers,
                      @Nullable SetMultimap<Term, TermAnnotation> termAnn,
                      @Nullable Set<QueryAnnotation> queryAnn,
                      @Nullable SetMultimap<Triple, TripleAnnotation> tripleAnn) {
        this.list = list;
        this.modifiers = createModifierSet(modifiers);
        this.modifiersView = this.modifiers.getLockedView();
        this.termAnnotations = termAnn == null ? HashMultimap.create() : termAnn;
        this.queryAnnotations = queryAnn == null ? new HashSet<>() : queryAnn;
        this.tripleAnnotations = tripleAnn == null ? HashMultimap.create() : tripleAnn;
        this.cache = new CQueryCache(this);
    }

    public CQueryData(@Nonnull CQueryData other) {
        this.list = new ArrayList<>(other.list);
        this.modifiers = createModifierSet(other.modifiers);
        this.modifiersView = this.modifiers.getLockedView();
        this.queryAnnotations = new HashSet<>(other.queryAnnotations);
        this.termAnnotations = HashMultimap.create(other.termAnnotations);
        this.tripleAnnotations = HashMultimap.create(other.tripleAnnotations);
        this.nextHiddenId = null;
        this.cache = new CQueryCache(this, other.cache);
    }

    public @Nonnull CQueryData toExclusive() {
        assert references.get() >= 0;
        return references.decrementAndGet() == 0 ? this : new CQueryData(this);
    }

    public @Nonnull CQueryData attach() {
        references.incrementAndGet();
        return this;
    }

    public void detach() {
        references.decrementAndGet();
    }

    public int nextHiddenId() {
        if (nextHiddenId == null)
            nextHiddenId = new AtomicInteger((int) (Math.random() * 10000));
        return nextHiddenId.getAndIncrement();
    }
}
