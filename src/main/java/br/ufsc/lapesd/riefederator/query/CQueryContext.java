package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.prefix.PrefixDict;
import br.ufsc.lapesd.riefederator.model.prefix.StdPrefixDict;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.query.modifiers.Modifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public abstract class CQueryContext {
    private static Logger logger = LoggerFactory.getLogger(CQueryContext.class);

    public abstract List<Object> queryData();

    public @Nonnull List<Object> asList(Object... objects) {
        return Arrays.asList(objects);
    }

    public @Nonnull CQuery build() {
        List<Object> list = queryData();
        return createQuery(list.toArray());
    }

    public static @Nonnull CQuery createQuery(Object... termAndAnnotations) {
        CQuery.Builder builder = CQuery.builder(termAndAnnotations.length / 3);
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
                boolean isTermAnnotation = next instanceof TermAnnotation;
                boolean isTripleAnnotation = next instanceof TripleAnnotation;
                if (isTermAnnotation && isTripleAnnotation)
                    logger.info("{} will annotate both the object and the whole previous triple!",
                            next);
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
                if (next instanceof Modifier)
                    builder.modifier((Modifier) next);
                if (next instanceof PrefixDict) {
                    if (prefixDict != null)
                        logger.warn("Will overwrite PrefixDict {} with {}", prefixDict, next);
                    prefixDict = (PrefixDict) next;
                }
            }
        }
        builder.prefixDict(prefixDict == null ? StdPrefixDict.STANDARD : prefixDict);
        return builder.build();
    }
}
