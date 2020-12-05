package br.ufsc.lapesd.riefederator.util.parse.iterators;

import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.jena.model.term.node.JenaNodeTermFactory;
import br.ufsc.lapesd.riefederator.model.NTParseException;
import br.ufsc.lapesd.riefederator.model.RDFUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;

public class HDTJenaTripleIterator implements JenaTripleIterator {
    private final static Logger logger = LoggerFactory.getLogger(HDTJenaTripleIterator.class);

    protected final @Nullable Object source;
    private final @Nonnull IteratorTripleString it;
    private @Nullable Triple next = null;

    public HDTJenaTripleIterator(@Nullable Object source, @Nonnull IteratorTripleString it) {
        this.source = source;
        this.it = it;
    }

    @Override public void close() { }

    @Override public boolean hasNext() {
        while (next == null && it.hasNext()) {
            TripleString ts = it.next();
            try {
                next = new Triple(parse(ts.getSubject()), parse(ts.getPredicate()),
                        parse(ts.getObject()));
            } catch (NTParseException e) {
                logger.warn("Ignoring unparseable HDT triple {} from source {}",
                        ts, source);
            }
        }
        return next != null;
    }

    private @Nonnull Node parse(@Nonnull CharSequence seq) throws NTParseException {
        if (Character.isLetter(seq.charAt(0))) {
            return NodeFactory.createURI(seq.toString());
        } else {
            JenaNodeTermFactory fac = JenaNodeTermFactory.INSTANCE;
            return JenaWrappers.toJenaNode(RDFUtils.fromNT(seq.toString(), fac));
        }
    }

    @Override public @Nonnull Triple next() {
        if (!hasNext()) throw new NoSuchElementException();
        Triple next = this.next;
        this.next = null;
        return next;
    }
}
