package br.ufsc.lapesd.riefederator.query.impl;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQueryCache;
import br.ufsc.lapesd.riefederator.query.annotations.QueryAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class CQueryData {
    public final @Nonnull AtomicInteger references = new AtomicInteger(0);
    public @Nonnull List<Triple> list;
    public @Nonnull Set<Modifier> modifiers;

    public @Nonnull SetMultimap<Term, TermAnnotation> termAnnotations;
    public @Nonnull Set<QueryAnnotation> queryAnnotations;
    public @Nonnull SetMultimap<Triple, TripleAnnotation> tripleAnnotations;
    private @Nullable AtomicInteger nextHiddenId;
    public final @Nonnull CQueryCache cache;

    public CQueryData(@Nonnull List<Triple> list) {
        this(list, null, null, null, null);
    }

    public CQueryData(@Nonnull List<Triple> list, @Nullable Set<Modifier> modifiers,
                      @Nullable SetMultimap<Term, TermAnnotation> termAnn,
                      @Nullable Set<QueryAnnotation> queryAnn,
                      @Nullable SetMultimap<Triple, TripleAnnotation> tripleAnn) {
        this.list = list;
        this.modifiers = modifiers == null ? new HashSet<>() : modifiers;
        this.termAnnotations = termAnn == null ? HashMultimap.create() : termAnn;
        this.queryAnnotations = queryAnn == null ? new HashSet<>() : queryAnn;
        this.tripleAnnotations = tripleAnn == null ? HashMultimap.create() : tripleAnn;
        this.cache = new CQueryCache(this);
    }

    public CQueryData(@Nonnull CQueryData other) {
        this.list = new ArrayList<>(other.list);
        this.modifiers = new HashSet<>(other.modifiers);
        this.queryAnnotations = new HashSet<>(other.queryAnnotations);
        this.termAnnotations = HashMultimap.create(other.termAnnotations);
        this.tripleAnnotations = HashMultimap.create(other.tripleAnnotations);
        this.nextHiddenId = null;
        this.cache = new CQueryCache(this, other.cache);
    }

    public @Nonnull CQueryData toExclusive() {
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
            nextHiddenId = new AtomicInteger((int)(Math.random()*10000));
        return nextHiddenId.getAndIncrement();
    }
}
