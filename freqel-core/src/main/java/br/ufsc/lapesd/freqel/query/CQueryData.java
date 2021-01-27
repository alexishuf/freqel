package br.ufsc.lapesd.freqel.query;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.annotations.QueryAnnotation;
import br.ufsc.lapesd.freqel.query.annotations.TermAnnotation;
import br.ufsc.lapesd.freqel.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.freqel.query.modifiers.Modifier;
import br.ufsc.lapesd.freqel.query.modifiers.ModifiersSet;
import br.ufsc.lapesd.freqel.util.RemoveableEmptySet;
import br.ufsc.lapesd.freqel.util.RemoveableEmptySetMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CQueryData {
    public static final @Nonnull Set<QueryAnnotation> EMPTY_QUERY_ANNS = RemoveableEmptySet.get();
    public static final @Nonnull SetMultimap<Term, TermAnnotation> EMPTY_TERM_ANNS
            = RemoveableEmptySetMultimap.get();
    public static final @Nonnull SetMultimap<Triple, TripleAnnotation> EMPTY_TRIPLE_ANNS
            = RemoveableEmptySetMultimap.get();

    private int references;
    public @Nonnull List<Triple> list;
    public final @Nonnull NotifyingModifierSet modifiers;
    public final @Nonnull ModifiersSet modifiersView;

    public @Nonnull SetMultimap<Term, TermAnnotation> termAnns;
    public @Nonnull Set<QueryAnnotation> queryAnns;
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
        this.termAnns = termAnn == null ? EMPTY_TERM_ANNS : termAnn;
        this.queryAnns = queryAnn == null ? EMPTY_QUERY_ANNS : queryAnn;
        this.tripleAnns = tripleAnn == null ? EMPTY_TRIPLE_ANNS : tripleAnn;
        this.cache = new CQueryCache(this);
    }

    public CQueryData(@Nonnull CQueryData o) {
        this.references = 1;
        this.list = new ArrayList<>(o.list);
        this.modifiers = new NotifyingModifierSet(o.modifiers);
        this.modifiersView = this.modifiers.getLockedView();
        this.queryAnns = o.queryAnns == EMPTY_QUERY_ANNS ? EMPTY_QUERY_ANNS
                                                         : new HashSet<>(o.queryAnns);
        this.termAnns = o.termAnns == EMPTY_TERM_ANNS ? EMPTY_TERM_ANNS
                                                      : HashMultimap.create(o.termAnns);
        this.tripleAnns = o.tripleAnns == EMPTY_TRIPLE_ANNS ? EMPTY_TRIPLE_ANNS
                                                            : HashMultimap.create(o.tripleAnns);
        this.nextHiddenId = null;
        this.cache = new CQueryCache(this, o.cache);
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

    public @Nonnull SetMultimap<Term, TermAnnotation> termAnns() {
        if (termAnns == EMPTY_TERM_ANNS)
            termAnns = HashMultimap.create();
        return termAnns;
    }
    public @Nonnull Set<QueryAnnotation> queryAnns() {
        if (queryAnns == EMPTY_QUERY_ANNS)
            queryAnns = new HashSet<>();
        return queryAnns;
    }
    public @Nonnull SetMultimap<Triple, TripleAnnotation> tripleAnns() {
        if (tripleAnns == EMPTY_TRIPLE_ANNS)
            tripleAnns = HashMultimap.create();
        return tripleAnns;
    }
}
