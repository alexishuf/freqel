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
    private int references;
    public @Nonnull List<Triple> list;
    public final @Nonnull NotifyingModifierSet modifiers;
    public final @Nonnull ModifiersSet modifiersView;

    public @Nonnull SetMultimap<Term, TermAnnotation> termAnns;
    public @Nonnull Set<QueryAnnotation> queryAnnotations;
    public @Nonnull SetMultimap<Triple, TripleAnnotation> tripleAnns;
    private @Nullable AtomicInteger nextHiddenId;
    public final @Nonnull CQueryCache cache;

    class NotifyingModifierSet extends ModifiersSet {
        boolean silenced = false;

        public NotifyingModifierSet(@Nullable Collection<? extends Modifier> collection) {
            super(collection);
        }

        @Override protected void added(@Nonnull Modifier modifier) {
            if (!silenced)
                cache.notifyModifierChange(modifier.getClass());
        }
        @Override protected void removed(@Nonnull Modifier modifier) {
            if (!silenced)
                cache.notifyModifierChange(modifier.getClass());
        }
    }

    public CQueryData(@Nonnull List<Triple> list) {
        this(list, null, null, null, null);
    }

    public CQueryData(@Nonnull List<Triple> list, @Nullable Set<Modifier> modifiers,
                      @Nullable SetMultimap<Term, TermAnnotation> termAnn,
                      @Nullable Set<QueryAnnotation> queryAnn,
                      @Nullable SetMultimap<Triple, TripleAnnotation> tripleAnn) {
        this.references = 1;
        this.list = list;
        this.modifiers = new NotifyingModifierSet(modifiers);
        this.modifiersView = this.modifiers.getLockedView();
        this.termAnns = termAnn == null ? HashMultimap.create() : termAnn;
        this.queryAnnotations = queryAnn == null ? new HashSet<>() : queryAnn;
        this.tripleAnns = tripleAnn == null ? HashMultimap.create() : tripleAnn;
        this.cache = new CQueryCache(this);
    }

    public CQueryData(@Nonnull CQueryData other) {
        this.references = 1;
        this.list = new ArrayList<>(other.list);
        this.modifiers = new NotifyingModifierSet(other.modifiers);
        this.modifiersView = this.modifiers.getLockedView();
        this.queryAnnotations = new HashSet<>(other.queryAnnotations);
        this.termAnns = HashMultimap.create(other.termAnns);
        this.tripleAnns = HashMultimap.create(other.tripleAnns);
        this.nextHiddenId = null;
        this.cache = new CQueryCache(this, other.cache);
    }

    public synchronized @Nonnull CQueryData toExclusive() {
        if (references == 1)
            return this;
        return new CQueryData(this);
    }

    public synchronized @Nonnull CQueryData attach() {
        ++references;
        return this;
    }

    public int nextHiddenId() {
        if (nextHiddenId == null)
            nextHiddenId = new AtomicInteger((int) (Math.random() * 10000));
        return nextHiddenId.getAndIncrement();
    }
}
