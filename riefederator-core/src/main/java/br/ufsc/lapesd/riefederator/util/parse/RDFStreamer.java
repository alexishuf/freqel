package br.ufsc.lapesd.riefederator.util.parse;

import org.apache.jena.riot.system.StreamRDF;

import javax.annotation.Nonnull;

public interface RDFStreamer {

    /**
     * Parses RDF at given source and feeds it to the given streamRDF.
     *
     * Implementations should catch {@link InterruptStreamException} without rethrowing it or
     * logging errors. Such exception indicates parsing (and streaming) should be stopped.
     *
     * @throws SourceIterationException if the source cannot be streamed
     */
    void stream(@Nonnull Object source, @Nonnull StreamRDF streamRDF) throws SourceIterationException;

    /**
     * True iff the source can be streamed with this implementation.
     */
    boolean canStream(@Nonnull Object source);

    void attachTo(@Nonnull RDFIterationDispatcher dispatcher);
}
