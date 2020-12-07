package br.ufsc.lapesd.riefederator.hdt.util;

import br.ufsc.lapesd.riefederator.algebra.Cardinality;
import br.ufsc.lapesd.riefederator.jena.JenaWrappers;
import br.ufsc.lapesd.riefederator.jena.model.term.JenaTermFactory;
import br.ufsc.lapesd.riefederator.jena.model.term.node.JenaNodeTermFactory;
import br.ufsc.lapesd.riefederator.model.NTParseException;
import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.term.Term;
import br.ufsc.lapesd.riefederator.model.term.factory.TermFactory;
import br.ufsc.lapesd.riefederator.model.term.std.StdTermFactory;
import br.ufsc.lapesd.riefederator.util.parse.RDFIterationDispatcher;
import br.ufsc.lapesd.riefederator.util.parse.TriplesEstimate;
import br.ufsc.lapesd.riefederator.util.parse.iterators.JenaTripleIterator;
import com.google.common.base.Preconditions;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.rdfhdt.hdt.enums.ResultEstimationType;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.rdf.TripleWriter;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static org.rdfhdt.hdt.enums.ResultEstimationType.EXACT;
import static org.rdfhdt.hdt.enums.ResultEstimationType.MORE_THAN;

public class HDTUtils {
    private static final Logger logger = LoggerFactory.getLogger(HDTUtils.class);
    private static final @Nonnull String BASE_URI = "urn:relative:";
    private static final Pattern INDEX_SUFFIX = Pattern.compile("\\.index(\\.v\\d+-\\d+)?$");

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

    public static @Nonnull String toHDTQueryTerm(@Nonnull Term term) {
        if (term.isURI()) return term.asURI().getURI();
        else if (term.isBlank() || term.isVar()) return "";
        else return RDFUtils.toNT(term);
    }

    public static @Nonnull String toHDTTerm(@Nonnull Node node) {
        return node.isURI() ? node.getURI() : RDFUtils.toNT(JenaWrappers.fromJena(node));
    }
    public static @Nonnull String toHDTTerm(@Nonnull Term term) {
        return term.isURI() ? term.asURI().getURI() : RDFUtils.toNT(term);
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
    public static @Nonnull Node toNode(@Nonnull CharSequence cs) throws NTParseException {
        return JenaWrappers.toJenaNode(toTerm(cs, JenaNodeTermFactory.INSTANCE));
    }
    public static @Nonnull RDFNode toRDFNode(@Nonnull CharSequence cs) throws NTParseException {
        return JenaWrappers.toJena(toTerm(cs, JenaTermFactory.INSTANCE));
    }
    public static @Nonnull TripleString toTripleString(@Nonnull br.ufsc.lapesd.riefederator.model.Triple triple) {
        return new TripleString(toHDTTerm(triple.getSubject()), toHDTTerm(triple.getPredicate()),
                                toHDTTerm(triple.getObject()));
    }
    public static @Nonnull TripleString toTripleString(@Nonnull Triple triple) {
        return new TripleString(toHDTTerm(triple.getSubject()), toHDTTerm(triple.getPredicate()),
                                toHDTTerm(triple.getObject()));
    }
    public static @Nonnull TripleString toTripleString(@Nonnull Statement stmt) {
        return toTripleString(stmt.asTriple());
    }
    public static @Nonnull br.ufsc.lapesd.riefederator.model.Triple
    toTriple(@Nonnull TripleString ts, @Nonnull TermFactory termFactory) throws NTParseException {
        return new br.ufsc.lapesd.riefederator.model.Triple(
                          toTerm(ts.getSubject(), termFactory),
                          toTerm(ts.getPredicate(), termFactory),
                          toTerm(ts.getObject(), termFactory));
    }
    public static @Nonnull Triple toJenaTriple(@Nonnull TripleString ts) throws NTParseException {
        return new Triple(toNode(ts.getSubject()), toNode(ts.getPredicate()),
                          toNode(ts.getObject()));
    }
    public static @Nonnull Statement toStatement(@Nonnull TripleString ts) throws NTParseException {
        Property property = ResourceFactory.createProperty(ts.getPredicate().toString());
        return ResourceFactory.createStatement(toRDFNode(ts.getSubject()).asResource(), property,
                                               toRDFNode(ts.getObject()));
    }

    public static class JenaTripleStringIterator implements IteratorTripleString, AutoCloseable {
        private @Nonnull Collection<?> sources;
        private @Nonnull RDFIterationDispatcher dispatcher;
        private @Nonnull JenaTripleIterator it;
        private final @Nonnull TriplesEstimate estimate;

        public JenaTripleStringIterator(@Nonnull Collection<?> sources,
                                        @Nonnull RDFIterationDispatcher dispatcher) {
            this.sources = sources;
            this.dispatcher = dispatcher;
            this.estimate = dispatcher.estimateAll(sources);
            this.it = dispatcher.parseAll(sources);
        }

        @Override public void close() {
            it.close();
        }

        @Override public void goToStart() {
            it.close();
            it = dispatcher.parseAll(sources);
        }

        @Override public long estimatedNumResults() {
            return estimate.getEstimate();
        }

        @Override public ResultEstimationType numResultEstimation() {
            return estimate.getIgnoredSources() == 0 ? EXACT : MORE_THAN;
        }

        @Override public boolean hasNext() {
            return it.hasNext();
        }

        @Override public @Nonnull TripleString next() {
            if (!hasNext()) throw new NoSuchElementException();
            return toTripleString(it.next());
        }
    }

    /**
     * Creates an HDT file from a set of sources. Sources can be anything handled by the
     * default {@link RDFIterationDispatcher}
     *
     * If a destination file is provided, the HDT will be saved there and will be memory-mapped.
     * Else, the HDT is kept fully in memory. In any case, the returned HDT object is should
     * be closed by the caller.
     *
     * @param sources: list of sources, each source can be of a different class
     * @param dst: destination file or null if the HDT should be kept strictly on memory
     * @param index if true, and destination is non-null, will generate indexes for query the HDT
     * @param format: passed to HDT library
     * @return An in-memory HDT object.
     */
    public static @Nonnull HDT doGenerateHDT(@Nonnull Collection<?> sources,
                                             @Nullable File dst, boolean index,
                                             @Nonnull HDTOptions format) throws IOException {
        Preconditions.checkArgument(sources.size() > 0, "At least one source required");
        String path = dst == null ? null : dst.getAbsolutePath();
        if (dst != null) {
            File parent = dst.getParentFile();
            if (!parent.exists() && !parent.mkdirs())
                throw new IOException("Failed to mkdir "+parent.getAbsolutePath());
            if (parent.exists() && !parent.isDirectory())
                throw new IOException(parent.getAbsolutePath()+" exsists but is not a dir");
            if (dst.exists() && !dst.isFile())
                throw new IOException("Cannot overwrite non-file "+path+" with an HDT file");
            else if (dst.exists())
                logger.info("Overwriting {} with new HDT file", path);
        }

        RDFIterationDispatcher dispatcher = RDFIterationDispatcher.get();
        try (JenaTripleStringIterator it = new JenaTripleStringIterator(sources, dispatcher)) {
            HDT hdt;
            if (dst != null) {
                try (TripleWriter writer = HDTManager.getHDTWriter(path, BASE_URI, format)) {
                    while (it.hasNext())
                        writer.addTriple(it.next());
                } catch (IOException|RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException("Unexpected "+e.getClass().getSimpleName(), e);
                }
                if (index)
                    hdt = HDTManager.mapIndexedHDT(path, createProgress("Indexing", path));
                else
                    hdt = HDTManager.mapHDT(path, createProgress("Memory-mapping", path));
            } else {
                 hdt = HDTManager.generateHDT(it, BASE_URI, format,
                         createProgress("Generating HDT in-memory from", sources));
            }
            return hdt;
        } catch (ParserException e) {
            throw new IOException("Badly formatted triple", e);
        }
    }

    private static @Nonnull ProgressListener
    createProgress(@Nonnull String action, @Nonnull Object subject) {
        return new LoggerHDTProgressListener(logger, action+" "+subject.toString());
    }

    public static @Nonnull HDT generateHDT(Object... sources) throws IOException {
        return doGenerateHDT(asList(sources), null, false, new HDTSpecification());
    }

    public static @Nonnull HDT saveHDT(@Nonnull Collection<?> sources, @Nonnull File destination,
                                       boolean index) throws IOException {
        return doGenerateHDT(sources, destination, index, new HDTSpecification());
    }
    public static @Nonnull HDT saveHDT(@Nonnull File destination, boolean index,
                                       @Nonnull Object... sources) throws IOException {
        return doGenerateHDT(asList(sources), destination, index, new HDTSpecification());
    }

    public static @Nonnull HDT mapIndexed(@Nonnull File file) throws IOException {
        return HDTManager.mapIndexedHDT(file.getAbsolutePath(), NULL_LISTENER);
    }
    public static @Nonnull HDT map(@Nonnull File file) throws IOException {
        return HDTManager.mapHDT(file.getAbsolutePath(), NULL_LISTENER);
    }

    public static @Nonnull Stream<Term> streamTerm(@Nonnull IteratorTripleString it,
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
        if (it.numResultEstimation() == EXACT)
            sit = Spliterators.spliterator(termIt, it.estimatedNumResults(), Spliterator.NONNULL);
        else
            sit = Spliterators.spliteratorUnknownSize(termIt, Spliterator.NONNULL);
        return StreamSupport.stream(sit, false);
    }

    public static boolean deleteWithIndex(@Nonnull File hdtFile) {
        try {
            forceDeleteWithIndex(hdtFile);
            return true;
        } catch (IOException e) { return false; }
    }

    public static void forceDeleteWithIndex(@Nonnull File hdtFile) throws IOException {
        List<File> failed = null;
        if (!hdtFile.delete())
            (failed == null ? failed = new ArrayList<>() : failed).add(hdtFile);
        for (File index : getIndexFiles(hdtFile)) {
            if (!index.delete())
                (failed == null ? failed = new ArrayList<>() : failed).add(index);
        }
        if (failed != null)
            throw new IOException("Failed to remove these files "+failed);
    }

    private static @Nonnull File[] getIndexFiles(@Nonnull File hdtFile) {
        File[] files = hdtFile.getParentFile().listFiles((d, n) -> n.startsWith(hdtFile.getName())
                && INDEX_SUFFIX.matcher(n).find());
        return files == null ? new File[0] : files;
    }

    public static class NullProgressListener implements ProgressListener  {
        @Override public void notifyProgress(float level, String message) { }
    }
    public static ProgressListener NULL_LISTENER = new NullProgressListener();

}
