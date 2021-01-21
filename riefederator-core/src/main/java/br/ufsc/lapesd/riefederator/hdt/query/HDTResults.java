package br.ufsc.lapesd.riefederator.hdt.query;

import br.ufsc.lapesd.riefederator.hdt.HDTUtils;
import br.ufsc.lapesd.riefederator.model.NTParseException;
import br.ufsc.lapesd.riefederator.model.Triple;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.std.StdTermFactory;
import br.ufsc.lapesd.riefederator.query.results.AbstractResults;
import br.ufsc.lapesd.riefederator.query.results.Results;
import br.ufsc.lapesd.riefederator.query.results.Solution;
import br.ufsc.lapesd.riefederator.query.results.impl.ArraySolution;
import br.ufsc.lapesd.riefederator.util.indexed.FullIndexSet;
import br.ufsc.lapesd.riefederator.util.indexed.IndexSet;
import org.rdfhdt.hdt.triples.TripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

import static br.ufsc.lapesd.riefederator.hdt.HDTUtils.toCardinality;

public class HDTResults extends AbstractResults implements Results {
    private static final Logger logger = LoggerFactory.getLogger(HDTResults.class);
    private final @Nonnull Iterator<TripleString> hdtIt;
    private final @Nonnull ArraySolution.ValueFactory factory;
    private final @Nonnull String[] names;
    private Solution next;
    private int pendingEstimate;
    private final @Nonnull List<Term> tempValues = new ArrayList<>();


    private static @Nullable String getName(@Nonnull Term term) {
        return term.isVar() ? term.asVar().getName() : null;
    }

    protected static IndexSet<String> getVars(@Nonnull Triple query) {
        FullIndexSet<String> set = new FullIndexSet<>(3);
        String name;
        if ((name = getName(query.getSubject()  )) != null) set.add(name);
        if ((name = getName(query.getPredicate())) != null) set.add(name);
        if ((name = getName(query.getObject()   )) != null) set.add(name);
        return set;
    }

    public HDTResults(@Nonnull Iterator<TripleString> it, @Nonnull Triple query) {
        this(getVars(query), it, query);
    }

    public HDTResults(@Nonnull Collection<String> varNames, @Nonnull Iterator<TripleString> it,
                      @Nonnull Triple query) {
        super(varNames);
        this.hdtIt = it;
        this.pendingEstimate = (int)Math.min(toCardinality(it).getValue(0), Integer.MAX_VALUE);
        this.factory = ArraySolution.forVars(varNames);
        this.names = new String[] {getName(query.getSubject()), getName(query.getPredicate()),
                                   getName(query.getObject())};
    }

    @Override public int getReadyCount() {
        return (next != null ? 1 : 0) + pendingEstimate;
    }

    @Override public boolean hasNext() {
        if (next == null && hdtIt.hasNext()) {
            TripleString ts = hdtIt.next();
            try {
                next = createSolution(ts);
            } catch (NTParseException e) {
                assert false : "HDT iterator spit invalid data";
                logger.error("{}.createSolution({}) failed to parse some terms. Variables: {}. " +
                             "Ignoring TripleString", this, ts, names, e);
            } finally {
                if (pendingEstimate > 0)
                    --pendingEstimate;
            }
        }
        return next != null;
    }

    private @Nonnull Solution createSolution(@Nonnull TripleString ts) throws NTParseException {
        StdTermFactory termFac = StdTermFactory.INSTANCE;
        tempValues.clear();
        for (String name : factory.getVarNames()) {
            if (names[0] != null && name.equals(names[0]))
                tempValues.add(HDTUtils.toTerm(ts.getSubject(), termFac));
            else if (names[1] != null && name.equals(names[1]))
                tempValues.add(HDTUtils.toTerm(ts.getPredicate(), termFac));
            else if (names[2] != null && name.equals(names[2]))
                tempValues.add(HDTUtils.toTerm(ts.getObject(), termFac));
        }
        return factory.fromValues(tempValues);
    }

    @Override public @Nonnull Solution next() {
        if (!hasNext()) throw new NoSuchElementException();
        Solution next = this.next;
        this.next = null;
        return next;
    }

    @Override public void close() {/* nothing */}
}
