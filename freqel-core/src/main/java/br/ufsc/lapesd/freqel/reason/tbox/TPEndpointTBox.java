package br.ufsc.lapesd.freqel.reason.tbox;

import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.Var;
import br.ufsc.lapesd.freqel.model.term.std.StdVar;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.modifiers.Ask;
import br.ufsc.lapesd.freqel.query.results.Results;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static br.ufsc.lapesd.freqel.V.RDFS.subClassOf;
import static br.ufsc.lapesd.freqel.V.RDFS.subPropertyOf;
import static br.ufsc.lapesd.freqel.query.parse.CQueryContext.createQuery;
import static java.util.Spliterators.spliteratorUnknownSize;

public class TPEndpointTBox implements TBox {
    private static final Var x = new StdVar("x");
    private boolean closed = false;
    private final TPEndpoint endpoint;
    private final boolean closeEndpoint;

    protected TPEndpointTBox(TPEndpoint endpoint, boolean closeEndpoint) {
        this.endpoint = endpoint;
        this.closeEndpoint = closeEndpoint;
    }

    public static @Nonnull TPEndpointTBox create(@Nonnull TPEndpoint ep) {
        return new TPEndpointTBox(ep, true);
    }
    public static @Nonnull TPEndpointTBox sharing(@Nonnull TPEndpoint ep) {
        return new TPEndpointTBox(ep, false);
    }

    @Override public @Nonnull TPEndpoint getEndpoint() {
        return endpoint;
    }

    private void addSubjects(@Nonnull Collection<Term> out,
                             @Nonnull Term predicate, @Nonnull Term object) {
        endpoint.query(createQuery(x, predicate, object)).forEachRemainingThenClose(s -> {
            Term sub = s.get(x);
            if (sub != null)
                out.add(sub);
        });
    }

    private @Nonnull Iterator<Term> iterateSubjects(@Nonnull Term predicate, @Nonnull Term object,
                                                    boolean includeSelf) {
        Set<Term> visited = new HashSet<>();
        ArrayDeque<Term> stack = new ArrayDeque<>();
        if (includeSelf) {
            stack.push(object);
        } else {
            addSubjects(stack, predicate, object);
            visited.add(object);
        }
        return new Iterator<Term>() {
            Term next = null;

            @Override public boolean hasNext() {
                while (next == null && !stack.isEmpty()) {
                    Term term = stack.removeLast();
                    if (visited.add(term))
                        addSubjects(stack, predicate, next = term);
                }
                return next != null;
            }

            @Override public @Nonnull Term next() {
                if (!hasNext()) throw new NoSuchElementException();
                Term next = this.next;
                this.next = null;
                assert next != null;
                return next;
            }
        };
    }

    private @Nonnull Stream<Term> streamSubjects(@Nonnull Term predicate, @Nonnull Term object,
                                                 boolean includeSelf) {
        int flags = Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.NONNULL;
        Iterator<Term> iterator = iterateSubjects(predicate, object, includeSelf);
        return StreamSupport.stream(spliteratorUnknownSize(iterator,flags), false);
    }

    @Override public @Nonnull Stream<Term> subClasses(@Nonnull Term term) {
        return streamSubjects(subClassOf, term, false);
    }

    @Override public @Nonnull Stream<Term> withSubClasses(@Nonnull Term term) {
        return streamSubjects(subClassOf, term, true);
    }

    @Override public @Nonnull Stream<Term> subProperties(@Nonnull Term term) {
        return streamSubjects(subPropertyOf, term, false);
    }

    @Override public @Nonnull Stream<Term> withSubProperties(@Nonnull Term term) {
        return streamSubjects(subPropertyOf, term, true);
    }

    @Override public boolean isSubProperty(@Nonnull Term subProperty, @Nonnull Term superProperty) {
        CQuery q = createQuery(subProperty, subPropertyOf, superProperty, Ask.INSTANCE);
        try (Results r = endpoint.query(q)) {
            return r.hasNext();
        }
    }

    @Override public boolean isSubClass(@Nonnull Term subClass, @Nonnull Term superClass) {
        CQuery q = createQuery(subClass, subClassOf, superClass, Ask.INSTANCE);
        try (Results r = endpoint.query(q)) {
            return r.hasNext();
        }
    }

    @Override public void close() {
        if (closed)
            return;
        closed = true;
        if (closeEndpoint)
            endpoint.close();
    }
}
