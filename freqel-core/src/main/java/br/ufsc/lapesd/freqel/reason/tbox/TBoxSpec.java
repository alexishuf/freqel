package br.ufsc.lapesd.freqel.reason.tbox;

import com.github.lapesd.rdfit.RIt;
import com.github.lapesd.rdfit.components.jena.JenaHelpers;
import com.github.lapesd.rdfit.source.RDFFile;
import com.github.lapesd.rdfit.source.RDFResource;
import com.github.lapesd.rdfit.util.impl.EternalCache;
import com.github.lapesd.rdfit.util.impl.RDFBlob;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.writer.NQuadsWriter;
import org.apache.jena.sparql.core.Quad;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.util.SimpleIRIMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.lapesd.rdfit.util.Utils.toASCIIString;
import static java.util.stream.Collectors.toList;

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

    public @Nonnull OWLOntology loadOWLOntology() {
        return loadOWLOntology(createOwlOntologyManager());
    }

    public  @Nonnull OWLOntologyManager createOwlOntologyManager() {
        OWLOntologyManager mgr = OWLManager.createOWLOntologyManager();
        try {
            // OWL and RDFS are language, trying to import them as ontologies will break stuff
            // in owlapi and their reasoners.
            mgr.createOntology(IRI.create("http://www.w3.org/2002/07/owl"));
            mgr.createOntology(IRI.create("http://www.w3.org/1999/02/22-rdf-syntax-ns#"));
            mgr.createOntology(IRI.create("http://www.w3.org/1999/02/22-rdf-syntax-ns"));
            // ignore failures to load owl:imports
            mgr.setOntologyLoaderConfiguration(mgr.getOntologyLoaderConfiguration()
                    .setMissingImportHandlingStrategy(MissingImportHandlingStrategy.SILENT));
        } catch (OWLOntologyCreationException e) {
            throw new TBoxLoadException("mgr.createOntology for OWL failed");
        }
        return mgr;
    }

    private static class OWLAPIFiles implements AutoCloseable {
        private final List<RDFFile> extracted = new ArrayList<>();
        private final List<SimpleIRIMapper> mappers = new ArrayList<>();
        private final List<RDFFile> sources = new ArrayList<>();
        private final OWLOntologyManager mgr;

        public OWLAPIFiles(@Nonnull OWLOntologyManager mgr,
                           @Nonnull List<?> sources) throws IOException {
            this.mgr = mgr;
            try {
                for (Map.Entry<String, RDFBlob> e : EternalCache.getDefault().dump().entrySet()) {
                    RDFFile f = RDFFile.createTemp(e.getValue());
                    extracted.add(f);
                    mappers.add(new SimpleIRIMapper(IRI.create(e.getKey()),
                                                    IRI.create(f.getFile())));
                }
                mappers.forEach(mgr.getIRIMappers()::add);
                for (Object source : sources) {
                    RDFFile file = RDFFile.createTemp();
                    this.sources.add(file);
                    try (FileOutputStream out = new FileOutputStream(file.getFile())) {
                        NQuadsWriter.write(out, RIt.iterateQuads(Quad.class, source));
                    }
                }
            } catch (IOException|RuntimeException|Error t) {
                close();
                throw t;
            }
        }

        public @Nonnull List<String> getSourceFilesURIs() {
            return sources.stream().map(f -> toASCIIString(f.getFile().toURI())).collect(toList());
        }

        @Override public void close() {
            mappers.forEach(mgr.getIRIMappers()::remove);
            Stream.concat(extracted.stream(), sources.stream()).forEach(RDFFile::close);
        }
    }

    public @Nonnull OWLOntology loadOWLOntology(@Nonnull OWLOntologyManager mgr) {
        File main = null;
        try (OWLAPIFiles extracted = new OWLAPIFiles(mgr, sources)) {
            main = File.createTempFile("onto_imports", ".nt");
            main.deleteOnExit();
            try (PrintStream out = new PrintStream(new FileOutputStream(main))) {
                out.println("@prefix owl: <http://www.w3.org/2002/07/owl#> .");
                out.printf("<file://%s> a owl:Ontology", main.getAbsolutePath());
                for (String uri : extracted.getSourceFilesURIs())
                    out.printf(";\n  owl:imports <%s> ", uri);
                out.println(".");
            }
            return mgr.loadOntologyFromOntologyDocument(main);
        } catch (IOException e) {
            throw new TBoxLoadException("Problem while writing importer ontology.", e);
        } catch (OWLOntologyCreationException e) {
            throw new TBoxLoadException("Failed to load importer ontology.", e);
        } finally {
            if (main != null && main.exists() && !main.delete())
                logger.warn("Ignoring failure to delete main temp file {}", main);
        }
    }
}
