package br.ufsc.lapesd.riefederator.hdt;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.model.NTParseException;
import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.factory.TermFactory;
import br.ufsc.lapesd.riefederator.model.term.std.StdTermFactory;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class HDTUtils {
    private static final Logger logger = LoggerFactory.getLogger(HDTUtils.class);

    public static @Nonnull Cardinality.Reliability
    toReliability(@Nonnull ResultEstimationType type) {
        switch (type) {
            case UNKNOWN: return Cardinality.Reliability.UNSUPPORTED;
            case APPROXIMATE: return Cardinality.Reliability.GUESS;
            case UP_TO: return Cardinality.Reliability.UPPER_BOUND;
            case MORE_THAN: return Cardinality.Reliability.LOWER_BOUND;
            case EXACT: return Cardinality.Reliability.EXACT;
            default:
                throw new UnsupportedOperationException("Unexpected ResultEstimationType "+type);
        }
    }


    public static @Nonnull Cardinality toCardinality(Iterator<TripleString> it) {
        if (it instanceof IteratorTripleString) {
            IteratorTripleString its = (IteratorTripleString)it;
            ResultEstimationType type = its.numResultEstimation();
            if (type == null || type == ResultEstimationType.UNKNOWN)
                return its.hasNext() ? Cardinality.NON_EMPTY : Cardinality.EMPTY;
            long value = Math.max(its.estimatedNumResults(), 0);
            return new Cardinality(toReliability(type), value);
        } else {
            return it.hasNext() ? Cardinality.EMPTY : Cardinality.NON_EMPTY;
        }
    }

    public static @Nonnull String toHDTQueryTerm(@Nonnull Term term) {
        if (term.isURI()) return term.asURI().getURI();
        else if (term.isBlank() || term.isVar()) return "";
        else return RDFUtils.toNT(term);
    }

    public static @Nonnull Term toTerm(@Nonnull CharSequence cs,
                                       @Nonnull TermFactory termFac) throws NTParseException {
        String string = cs.toString();
        char first = cs.charAt(0);
        if (first == '"' || Character.isDigit(first))
            return RDFUtils.fromNT(string, termFac);
        if (string.startsWith("_:"))
            return termFac.createBlank(string.substring(2));
        return termFac.createURI(string);
    }

    public static @Nonnull Stream<Term> streamTerm(@Nonnull Iterator<TripleString> it,
                                                   @Nonnull br.ufsc.lapesd.riefederator.model.Triple.Position position) {
        Iterator<Term> termIt = new Iterator<Term>() {
            private @Nullable Term next;

            @Override public boolean hasNext() {
                while (next == null && it.hasNext()) {
                    TripleString ts = it.next();
                    CharSequence cs;
                    if (position == br.ufsc.lapesd.riefederator.model.Triple.Position.SUBJ)
                        cs = ts.getSubject();
                    else if (position == br.ufsc.lapesd.riefederator.model.Triple.Position.PRED)
                        cs = ts.getPredicate();
                    else
                        cs = ts.getObject();
                    try {
                        next = toTerm(cs, StdTermFactory.INSTANCE);
                    } catch (NTParseException e) {
                        logger.error("Invalid string from HDT: {}. Ignoring triple {}", cs, ts);
                    }
                }
                return next != null;
            }

            @Override public Term next() {
                if (!hasNext()) throw new NoSuchElementException();
                Term next = this.next;
                assert next != null;
                this.next = null;
                return next;
            }
        };
        Spliterator<Term> sit;
        Cardinality cardinality = toCardinality(it);
        if (cardinality.getReliability() == Cardinality.Reliability.EXACT)
            sit = Spliterators.spliterator(termIt, cardinality.getValue(0), Spliterator.NONNULL);
        else
            sit = Spliterators.spliteratorUnknownSize(termIt, Spliterator.NONNULL);
        return StreamSupport.stream(sit, false);
    }

}
