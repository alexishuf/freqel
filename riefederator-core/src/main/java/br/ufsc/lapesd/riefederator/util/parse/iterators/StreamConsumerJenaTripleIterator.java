package br.ufsc.lapesd.riefederator.util.parse.iterators;

import br.ufsc.lapesd.riefederator.util.parse.InterruptStreamException;
import br.ufsc.lapesd.riefederator.util.parse.SourceIterationException;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFBase;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class StreamConsumerJenaTripleIterator implements JenaTripleIterator {
    private static final Node END_URI = NodeFactory.createURI("urn:riefederator:END");
    private static final Triple END_MARKER = new Triple(END_URI, END_URI, END_URI);
    private class Producer extends StreamRDFBase {
        boolean abort = false;

        @Override public void triple(Triple triple) {
            if (abort) {
                abort = false;
                throw new InterruptStreamException();
            }
            try {
                queue.put(triple);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override public void finish() {
            try {
                queue.put(END_MARKER);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    private final Producer producer = new Producer();
    private final BlockingQueue<Triple> queue = new ArrayBlockingQueue<>(2048);
    private @Nullable SourceIterationException exception;
    private @Nullable Triple next;

    public @Nonnull StreamRDF getStreamRDF() {
        return producer;
    }

    public void setException(@Nonnull SourceIterationException exception) {
        this.exception = exception;
    }

    @Override public void close() {
        producer.abort = true;
    }

    @Override public boolean hasNext() throws SourceIterationException {
        if (next == null) {
            try {
                next = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        assert next != null;
        if (exception != null)
            throw exception;
        return next != END_MARKER;
    }

    @Override public Triple next() throws SourceIterationException {
        if (!hasNext()) throw new NoSuchElementException();
        Triple result = this.next;
        this.next = null;
        return result;
    }
}
