package br.ufsc.lapesd.freqel.query.parse;

import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.model.prefix.PrefixDict;
import br.ufsc.lapesd.freqel.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.MutableCQuery;
import br.ufsc.lapesd.freqel.query.annotations.TermAnnotation;
import br.ufsc.lapesd.freqel.query.annotations.TripleAnnotation;
import br.ufsc.lapesd.freqel.query.modifiers.Modifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public abstract class CQueryContext {
    private static final Logger logger = LoggerFactory.getLogger(CQueryContext.class);

    public abstract List<Object> queryData();

    public @Nonnull List<Object> asList(Object... objects) {
        return Arrays.asList(objects);
    }

    public @Nonnull
    CQuery build() {
        List<Object> list = queryData();
        return createQuery(list.toArray());
    }

    public static class TermAnnotationPair {
        private final @Nonnull Term term;
        private final @Nonnull
        TermAnnotation annotation;

        public TermAnnotationPair(@Nonnull Term term, @Nonnull TermAnnotation annotation) {
            this.term = term;
            this.annotation = annotation;
        }

        public @Nonnull Term getTerm() {
            return term;
        }

        public @Nonnull TermAnnotation getAnnotation() {
            return annotation;
        }

        @Override
        public @Nonnull String toString() {
            return String.format("TermAnnotationPair(%s, %s)", term, annotation);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TermAnnotationPair)) return false;
            TermAnnotationPair that = (TermAnnotationPair) o;
            return getTerm().equals(that.getTerm()) &&
                    getAnnotation().equals(that.getAnnotation());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getTerm(), getAnnotation());
        }
    }

    public static @Nonnull TermAnnotationPair
    annotateTerm(@Nonnull Term term, @Nonnull TermAnnotation annotation) {
        return new TermAnnotationPair(term, annotation);
    }

    public static @Nonnull MutableCQuery createQuery(Object... termAndAnnotations) {
        return createQuery(false, termAndAnnotations);
    }

    public static @Nonnull MutableCQuery createTolerantQuery(Object... termAndAnnotations) {
        return createQuery(true, termAndAnnotations);
    }

    static @Nonnull MutableCQuery createQuery(boolean tolerant, Object... termAndAnnotations) {
        MutableCQuery query = new MutableCQuery(termAndAnnotations.length / 3);
        PrefixDict prefixDict = null;
        List<Term> window = new ArrayList<>();
        List<TermAnnotationPair> pending = new ArrayList<>();
        for (Object next : termAndAnnotations) {
            if (next instanceof Triple) {
                checkArgument(window.isEmpty(), "Found a triple within a window of 3 terms");
                query.add((Triple)next);
                for (TermAnnotationPair pair : pending)
                    query.annotate(pair.term, pair.annotation);
                pending.clear();
            } else if (next instanceof Term) {
                window.add((Term) next);
                if (window.size() == 3) {
                    query.add(new Triple(window.get(0), window.get(1), window.get(2)));
                    window.clear();
                    for (TermAnnotationPair pair : pending)
                        query.annotate(pair.term, pair.annotation);
                    pending.clear();
                }
            } else {
                boolean processed = false;
                boolean isTermAnnotation = next instanceof TermAnnotation;
                boolean isTripleAnnotation = next instanceof TripleAnnotation;
                if (isTermAnnotation && isTripleAnnotation)
                    logger.info("{} will annotate both the previous object and triple!", next);
                if (isTermAnnotation) {
                    Term term;
                    if (window.isEmpty()) {
                        checkState(!query.isEmpty(), "Received TermAnnotation before any term!");
                        term = query.get(query.size()-1).getObject();
                        query.annotate(term, (TermAnnotation) next);
                    } else {
                        term = window.get(window.size() - 1);
                        pending.add(new TermAnnotationPair(term, (TermAnnotation)next));
                    }
                    processed = true;
                }
                if (isTripleAnnotation) {
                    checkState(isTermAnnotation || !query.isEmpty(),
                            "Received TripleAnnotation before first triple is complete!");
                    if (window.isEmpty()) {
                        query.annotate(query.get(query.size() - 1), (TripleAnnotation) next);
                    } else if (!isTermAnnotation) {
                        throw new IllegalStateException("Annotation "+next+" only implements " +
                                "TripleAnnotation but is positioned after a subject or predicate");
                    }
                    processed = true;
                }
                if (next instanceof Modifier) {
                    query.mutateModifiers().add((Modifier) next);
                    processed = true;
                }
                if (next instanceof PrefixDict) {
                    if (prefixDict != null)
                        logger.warn("Will overwrite PrefixDict {} with {}", prefixDict, next);
                    prefixDict = (PrefixDict) next;
                    processed = true;
                }
                if (next instanceof TermAnnotationPair) {
                    TermAnnotationPair pair = (TermAnnotationPair) next;
                    query.annotate(pair.getTerm(), pair.getAnnotation());
                    processed = true;
                }
                if (!processed)
                    throw new IllegalArgumentException("Unsupported type for "+next);
            }
        }
        query.setPrefixDict(prefixDict == null ? StdPrefixDict.STANDARD : prefixDict);
        if (!tolerant)
            query.sanitizeProjectionStrict();
        return query;
    }
}
