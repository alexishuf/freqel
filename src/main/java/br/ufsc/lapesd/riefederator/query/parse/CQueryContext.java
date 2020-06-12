package br.ufsc.lapesd.riefederator.query.parse;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.CQuery;
import br.ufsc.lapesd.riefederator.query.TermAnnotation;
import br.ufsc.lapesd.riefederator.query.TripleAnnotation;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;

public abstract class CQueryContext {
    private static Logger logger = LoggerFactory.getLogger(CQueryContext.class);

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

    public static @Nonnull CQuery createQuery(Object... termAndAnnotations) {
        return createQuery(false, termAndAnnotations);
    }

    public static @Nonnull CQuery createTolerantQuery(Object... termAndAnnotations) {
        return createQuery(true, termAndAnnotations);
    }

    static @Nonnull CQuery createQuery(boolean tolerant, Object... termAndAnnotations) {
        CQuery.Builder builder = CQuery.builder(termAndAnnotations.length / 3)
                                       .allowExtraProjection(tolerant);
        PrefixDict prefixDict = null;
        List<Term> window = new ArrayList<>();
        for (Object next : termAndAnnotations) {
            if (next instanceof Term) {
                window.add((Term) next);
                if (window.size() == 3) {
                    builder.add(new Triple(window.get(0), window.get(1), window.get(2)));
                    window.clear();
                }
            } else {
                boolean processed = false;
                boolean isTermAnnotation = next instanceof TermAnnotation;
                boolean isTripleAnnotation = next instanceof TripleAnnotation;
                if (isTermAnnotation && isTripleAnnotation) {
                    logger.info("{} will annotate both the object and the whole previous triple!",
                            next);
                    processed = true;
                }
                if (isTermAnnotation) {
                    Term term;
                    if (window.isEmpty()) {
                        List<Triple> triples = builder.getList();
                        checkState(!triples.isEmpty(), "Received TermAnnotation before any term!");
                        term = triples.get(triples.size() - 1).getObject();
                    } else {
                        term = window.get(window.size() - 1);
                    }
                    builder.annotate(term, (TermAnnotation) next);
                }
                if (isTripleAnnotation) {
                    List<Triple> triples = builder.getList();
                    checkState(isTermAnnotation || !triples.isEmpty(),
                            "Received TripleAnnotation before first triple is complete!");
                    if (window.isEmpty()) {
                        builder.annotate(triples.get(triples.size() - 1), (TripleAnnotation) next);
                    } else {
                        checkState(isTermAnnotation, "Annotation "+next+" only implements " +
                                "TripleAnnotation but is positioned after a subject or predicate");
                    }
                }
                if (next instanceof Modifier) {
                    builder.modifier((Modifier) next);
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
                    builder.annotate(pair.getTerm(), pair.getAnnotation());
                    processed = true;
                }
                if (!processed)
                    throw new IllegalArgumentException("Unsupported type for "+next);
            }
        }
        builder.prefixDict(prefixDict == null ? StdPrefixDict.STANDARD : prefixDict);
        return builder.build();
    }
}
