package br.ufsc.lapesd.freqel.hdt.query;

import br.ufsc.lapesd.freqel.algebra.Cardinality;
import br.ufsc.lapesd.freqel.description.AskDescription;
import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.Freqel;
import br.ufsc.lapesd.freqel.hdt.HDTUtils;
import br.ufsc.lapesd.freqel.hdt.util.LoggerHDTProgressListener;
import br.ufsc.lapesd.freqel.model.Triple;
import br.ufsc.lapesd.freqel.query.CQuery;
import br.ufsc.lapesd.freqel.query.endpoint.AbstractTPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.Capability;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.decorators.EndpointDecorators;
import br.ufsc.lapesd.freqel.query.results.Results;
import br.ufsc.lapesd.freqel.query.results.ResultsUtils;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.freqel.cardinality.EstimatePolicy.canLocal;
import static java.nio.charset.StandardCharsets.UTF_8;

public class HDTEndpoint extends AbstractTPEndpoint implements TPEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(HDTEndpoint.class);
    private static final Pattern URI_RX = Pattern.compile("^[^:]+:");

    private final @Nonnull String name;
    private final @Nonnull HDT hdt;
    private final boolean closeHDT;
    private Boolean empty = null;
    private @Nullable Federation federation;

    public HDTEndpoint(@Nonnull HDT hdt, @Nonnull String name) {
        this(hdt, name, true);
    }

    public HDTEndpoint(@Nonnull HDT hdt, @Nonnull String name, boolean closeHDT) {
        super(e -> new AskDescription(e, 1024));
        this.hdt = hdt;
        this.name = name;
        this.closeHDT = closeHDT;
    }

    public static @Nonnull HDTEndpoint fromFile(@Nonnull String path) throws IOException {
        if (path.startsWith("file:")) {
            try {
                path = new URI(path).getPath();
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Bad URI:" + path);
            }
        }
        File file = new File(path);
        if (URI_RX.matcher(path).find() && !file.exists())
            throw new FileNotFoundException("Given path ("+path+") is a non-file URI");
        return fromFile(file);
    }
    public static @Nonnull HDTEndpoint fromFile(@Nonnull File file) throws IOException {
        ProgressListener listener = new LoggerHDTProgressListener(logger,
                "Memory-mapping indexed " + file.getAbsolutePath());
        HDT hdt = HDTManager.mapIndexedHDT(file.getAbsolutePath(), listener);
        return new HDTEndpoint(hdt, file.getAbsolutePath());
    }

    public static @Nonnull HDTEndpoint
    fromInputStream(@Nonnull String name, @Nonnull InputStream input) throws IOException {
        ProgressListener listener = new LoggerHDTProgressListener(logger,
                "Loading and indexing from stream " + name);
        HDT hdt = HDTManager.loadIndexedHDT(input, listener);
        return new HDTEndpoint(hdt, name);
    }

    public static @Nonnull HDTEndpoint fromResource(@Nonnull Class<?> refClass,
                                                    @Nonnull String resourcePath) {
        String path = "resource://" + refClass.getName().replace('.', '/')
                    + '/' + resourcePath;
        try (InputStream in = refClass.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalArgumentException("Resource "+resourcePath +
                                                   " not found relative to"+refClass);
            }
            return fromInputStream(path, in);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to load HDT from resource"+path, e);
        }
    }

    public static @Nullable HDTEndpoint tryFromFile(Object source) throws IOException {
        File file;
        if (source instanceof CharSequence && source.toString().startsWith("file:")) {
            try {
                file = Paths.get(new URI(source.toString())).toFile();
            } catch (URISyntaxException e) {
                logger.error("String starts with file: but is not a valid URI");
                return null;
            }
        } else if (source instanceof CharSequence) {
            file = new File(source.toString());
        } else if (source instanceof Path) {
            file = ((Path)source).toFile();
        } else if (source instanceof File) {
            file = (File) source;
        } else {
            logger.error("Cannot convert {} to a File", source);
            return null;
        }
        try (FileInputStream in = new FileInputStream(file)) {
            byte[] cookie = new byte[4];
            if (in.read(cookie) != 4 || !Arrays.equals(cookie, "$HDT".getBytes(UTF_8))) {
                logger.error("File {} does not have $HDT cookie", file);
                return null;
            }
        }
        return fromFile(file);
    }

    @Override public String toString() {
        return "HDTEndpoint{"+name+"}";
    }

    @Override public void close() {
        try {
            if (closeHDT)
                hdt.close();
        } catch (IOException e) {
            logger.error("{}.close(): Exception closing HDT object {}", this, hdt, e);
        }
        if (federation != null)
            federation.close();
    }

    public boolean isEmpty() {
        if (empty == null) {
            try {
                IteratorTripleString it = hdt.search(null, null, null);
                empty = it.hasNext();
            } catch (NotFoundException e) {
                assert false : "Unexpected exception";
                empty = true;
            }
        }
        return empty;
    }

    @Override public @Nonnull Results query(@Nonnull Triple query) {
        Iterator<TripleString> hdtIt = createIterator(query);
        return new HDTResults(hdtIt, query);
    }



    private @Nonnull Iterator<TripleString> createIterator(@Nonnull Triple query) {
        String s = HDTUtils.toHDTQueryTerm(query.getSubject());
        String p = HDTUtils.toHDTQueryTerm(query.getPredicate());
        String o = HDTUtils.toHDTQueryTerm(query.getObject());
        Iterator<TripleString> hdtIt;
        try {
            hdtIt = hdt.search(s, p, o);
        } catch (NotFoundException e) {
            hdtIt = Collections.emptyIterator();
        }
        return hdtIt;
    }

    @Override public @Nonnull Results query(@Nonnull CQuery query) {
        if (query.size() > 1) {
            if (federation == null)
                federation = Freqel.createFederation(EndpointDecorators.uncloseable(this));
            return federation.query(query);
        } else {
            return ResultsUtils.applyModifiers(query(query.get(0)), query.getModifiers());
        }
    }

    @Override public @Nonnull Cardinality estimate(@Nonnull CQuery query, int estimatePolicy) {
        if (query.isEmpty()) return Cardinality.EMPTY;
        if (canLocal(estimatePolicy)) {
            if (isEmpty()) return Cardinality.EMPTY;
            if (query.size() > 1) return Cardinality.UNSUPPORTED;
            return HDTUtils.toCardinality(createIterator(query.get(0)));
        }
        return Cardinality.UNSUPPORTED;
    }

    @Override public boolean hasRemoteCapability(@Nonnull Capability capability) {
        return capability == Capability.ASK || capability == Capability.LIMIT;
    }

    @Override public boolean hasCapability(@Nonnull Capability capability) {
        switch (capability) {
            case ASK:
            case PROJECTION:
            case DISTINCT:
            case LIMIT:
            case SPARQL_FILTER:
            case OPTIONAL:
            case CARTESIAN:
                return true;
            default: break;
        }
        return false;
    }
}
