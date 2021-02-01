package br.ufsc.lapesd.freqel.reason.tbox;

import com.github.lapesd.rdfit.RIt;
import com.github.lapesd.rdfit.components.jena.JenaHelpers;
import com.github.lapesd.rdfit.source.RDFResource;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class TBoxSpec implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(TBoxSpec.class);

    private final List<Object> sources = new ArrayList<>();
    private boolean fetchOwlImports = true;

    @CanIgnoreReturnValue
    public @Nonnull TBoxSpec fetchOwlImports(boolean value) {
        fetchOwlImports = value;
        return this;
    }

    @CheckReturnValue
    public boolean getFetchOwlImports() {
        return fetchOwlImports;
    }

    @CanIgnoreReturnValue
    public @Nonnull TBoxSpec add(@Nonnull Object source) {
        sources.add(source);
        return this;
    }

    /**
     * Adds a resource, relative to the given <code>cls</code>, as a source.
     *
     * @param cls Class that will be used to open the resource {@link InputStream}.
     * @param path Path to the resource, relative to cls;
     * @return The builder itself,
     * @throws TBoxLoadException if the resource was not found relative to cls
     */
    @CanIgnoreReturnValue
    public @Nonnull TBoxSpec addResource(@Nonnull Class<?> cls,
                                         @Nonnull String path) throws TBoxLoadException {
        try {
            sources.add(new RDFResource(cls, path));
        } catch (RuntimeException e) {
            throw new TBoxLoadException("Could not open resource "+path+" from class "+cls);
        }
        return this;
    }

    /**
     * Adds a bundled resource as input. The Language is guessed from the path.
     *
     * @param path path to the resource. If it has no '/', not even in the beggining,
     *             then a prefix of "br/ufsc/lapesd/freqel" will be prepended to the path.
     * @return The {@link TBoxSpec} itself.
     * @throws TBoxLoadException If the resource cannot be opened using the system classloader.
     */
    @CanIgnoreReturnValue
    public @Nonnull TBoxSpec addResource(@Nonnull String path) throws TBoxLoadException {
        String inPath = path;
        if (path.startsWith("/"))
            path = path.substring(1);
        else if (!path.startsWith("/"))
            path = "br/ufsc/lapesd/freqel/" + path;
        try {
            sources.add(new RDFResource(path));
        } catch (RuntimeException e) {
            throw new TBoxLoadException("Could not find resource "+inPath);
        }
        return this;
    }

    @Override
    public void close() {
        for (Object src : sources) {
            if (src instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) src).close();
                } catch (Exception e) {
                    logger.warn("Ignoring {}.close() exception", src, e);
                }
            }
        }
    }

    public @Nonnull Model loadModel() throws TBoxLoadException {
        Graph graph = JenaHelpers.toGraphImporting(RIt.iterateTriples(Triple.class, sources));
        return ModelFactory.createModelForGraph(graph);
    }

    public @Nonnull List<Object> getSources() {
        return sources;
    }
}
