package br.ufsc.lapesd.riefederator.reason.tbox;

import br.ufsc.lapesd.riefederator.jena.ModelUtils;
import br.ufsc.lapesd.riefederator.jena.TBoxLoader;
import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import br.ufsc.lapesd.riefederator.util.ExtractedResource;
import br.ufsc.lapesd.riefederator.util.ExtractedResources;
import br.ufsc.lapesd.riefederator.util.parse.RDFInputStream;
import br.ufsc.lapesd.riefederator.util.parse.RDFSyntax;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.jetbrains.annotations.NotNull;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.util.SimpleIRIMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.io.*;
import java.util.*;

import static org.apache.jena.riot.RDFLanguages.filenameToLang;

public class TBoxSpec implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(TBoxSpec.class);

    private List<String> uris = new ArrayList<>();
    private List<File> files = new ArrayList<>();
    private List<Model> models = new ArrayList<>();
    private List<RDFInputStream> streams = new ArrayList<>();
    boolean fetchOwlImports = true;

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
    public @Nonnull TBoxSpec addURI(@Nonnull String uri) {
        uris.add(uri);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull TBoxSpec addFile(@Nonnull File file) {
        files.add(file);
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull TBoxSpec addModel(@Nonnull Model model) {
        models.add(model);
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
        InputStream stream = cls.getResourceAsStream(path);
        if (stream == null)
            throw new TBoxLoadException("Could not open resource "+path+" from class "+cls);
        return addStream(stream, RDFLanguages.filenameToLang(path));
    }

    /**
     * Adds a bundled resource as input. The Language is guessed from the path.
     *
     * @param path path to the resource. If it has no '/', not even in the beggining,
     *             then a prefix of "br/ufsc/lapesd/riefederator" will be prepended to the path.
     * @return The {@link TBoxSpec} itself.
     * @throws TBoxLoadException If the resource cannot be opened using the system classloader.
     */
    @CanIgnoreReturnValue
    public @Nonnull TBoxSpec addResource(@Nonnull String path) throws TBoxLoadException {
        String inPath = path;
        if (path.startsWith("/"))
            path = path.substring(1);
        else if (path.indexOf('/') == -1)
            path = "br/ufsc/lapesd/riefederator" + path;
        InputStream stream = ClassLoader.getSystemClassLoader().getResourceAsStream(path);
        if (stream == null)
            throw new TBoxLoadException("Could not find resource "+inPath);
        return addStream(stream, filenameToLang(inPath));
    }

    @CanIgnoreReturnValue
    public @Nonnull TBoxSpec addStream(@Nonnull InputStream stream, @Nonnull Lang lang) {
        streams.add(new RDFInputStream(stream).setSyntax(RDFSyntax.fromJenaLang(lang)));
        return this;
    }
    @CanIgnoreReturnValue
    public @Nonnull TBoxSpec addStream(@Nonnull InputStream stream) {
        streams.add(new RDFInputStream(stream));
        return this;
    }

    @Override
    public void close() {
        for (RDFInputStream p : streams)
            p.close();
    }

    public @Nonnull Model loadModel() throws TBoxLoadException {
        TBoxLoader loader = new TBoxLoader().fetchingImports(fetchOwlImports);
        for (File file : files) {
            try {
                loader.addFile(file);
            } catch (Exception e) {
                throw new TBoxLoadException("Failed to load file "+file, e);
            }
        }
        for (String uri : uris) {
            try {
                loader.fetchOntology(uri);
            } catch (RuntimeException e) {
                throw new TBoxLoadException("Failed to load URI "+uri, e);
            }
        }
        try {
            for (Model model : models) loader.addModel(model);
        } catch (RuntimeException e) {
            throw new TBoxLoadException("Problem while loading from Model instances", e);
        }
        for (RDFInputStream ris : streams) {
            loader.addInputStream(ris);
        }

        return loader.getModel();
    }

    public @Nonnull List<Object> getAllSources() {
        ArrayList<Object> list = new ArrayList<>();
        list.addAll(files);
        list.addAll(uris);
        list.addAll(models);
        list.addAll(streams);
        return list;
    }

    private static final class FileHandle implements AutoCloseable {
        File file;
        boolean delete;

        private FileHandle(File file, boolean delete) {
            this.file = file;
            this.delete = delete;
        }

        @Override
        public void close() {
            if (delete) {
                if (!file.delete())
                    logger.error("Failed to delete temporary file " + file.getAbsolutePath());

            }
        }
    }

    public @Nonnull FileHandle handleFromFile(@Nonnull File file) {
        return new FileHandle(file, false);
    }
    public @Nonnull FileHandle handleFromTemp(@Nonnull File file) {
        return new FileHandle(file, true);
    }

    private FileHandle toTemp(@Nonnull Model model) throws TBoxLoadException {
        String s = "Model@"+System.identityHashCode(model);
        try {
            return handleFromTemp(ModelUtils.toTemp(model, false));
        } catch (IOException e) {
            throw new TBoxLoadException("Failed to generate temp file for "+s);
        }
    }


    private @Nonnull List<FileHandle> toFileHandles() {
        List<FileHandle> list = new ArrayList<>();
        for (Model model : models) list.add(toTemp(model));
        for (File file : files) list.add(handleFromFile(file));

        for (RDFInputStream ris : streams) {
            String suffix = "." + ris.getSyntaxOrGuess().getSuffix();
            File temp = null;
            try {
                temp = File.createTempFile("stream", suffix);
                temp.deleteOnExit();
                try (FileOutputStream out = new FileOutputStream(temp)) {
                    IOUtils.copy(ris.getInputStream(), out);
                }
            } catch (IOException e) {
                if (temp == null)
                    throw new TBoxLoadException("Failed to create tem file for stream "+ris);
                throw new TBoxLoadException("Failed to copy stream "+ris+" to file "+temp);
            }
            list.add(handleFromTemp(temp));
        }
        return list;
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

    private static class ResourceExtractedOntologies extends ExtractedResources {
        private static final @Nonnull Map<String, String> uri2resource;
        private final @Nonnull List<SimpleIRIMapper> mappers;
        private final @Nonnull OWLOntologyManager manager;

        static {
            Map<String, String> map = new LinkedHashMap<>();
            String dir = "br/ufsc/lapesd/riefederator";
            map.put("http://www.w3.org/1999/02/22-rdf-syntax-ns", dir+"/rdf.ttl");
            map.put("http://www.w3.org/2000/01/rdf-schema", dir+"/rdf-schema.ttl");
            map.put("http://www.w3.org/2002/07/owl", dir+"/owl.ttl");
            map.put("http://xmlns.com/foaf/0.1/", dir+"/foaf.rdf");
            map.put("http://www.w3.org/2006/time", dir+"/time.ttl");
            map.put("http://www.w3.org/ns/prov", dir+"/prov-o.ttl");
            map.put("http://www.w3.org/ns/prov-o", dir+"/prov-o.ttl");
            map.put("http://www.w3.org/2004/02/skos/core", dir+"/skos.rdf");
            map.put("http://purl.org/dc/elements/1.1/", dir+"/dcelements.ttl");
            map.put("http://purl.org/dc/dcam/", dir+"/dcam.ttl");
            map.put("http://purl.org/dc/dcmitype/", dir+"/dctype.ttl");
            map.put("http://purl.org/dc/terms/", dir+"/dcterms.ttl");
            uri2resource = map;
        }

        public ResourceExtractedOntologies(@NotNull OWLOntologyManager mgr) throws IOException {
            super(createExtractedResources());
            manager = mgr;
            mappers = new ArrayList<>(list.size());
            int i = 0;
            for (Map.Entry<String, String> e : uri2resource.entrySet()) {
                ExtractedResource ex = list.get(i++);
                assert ex.getResourcePath().equals(e.getValue());
                mappers.add(new SimpleIRIMapper(IRI.create(e.getKey()), IRI.create(ex.getFile())));
            }
            mappers.forEach(mgr.getIRIMappers()::add);
        }

        private static @Nonnull List<ExtractedResource>
        createExtractedResources() throws IOException {
            List<ExtractedResource> list = new ArrayList<>();
            try {
                for (String resource : uri2resource.values()) {
                    list.add(new ExtractedResource(resource));
                }
                return list;
            } catch (IOException e) {
                list.forEach(ExtractedResource::close);
                throw e;
            }
        }

        @Override
        public void close() {
            mappers.forEach(manager.getIRIMappers()::remove);
            super.close();
        }
    }

    public @Nonnull OWLOntology loadOWLOntology(@Nonnull OWLOntologyManager mgr) {
        List<FileHandle> handles = Collections.emptyList();
        try (ResourceExtractedOntologies extracted = new ResourceExtractedOntologies(mgr)) {
            handles = toFileHandles();
            File main = File.createTempFile("onto_imports", ".nt");
            main.deleteOnExit();
            try (PrintStream out = new PrintStream(new FileOutputStream(main))) {
                out.println("@prefix owl: <http://www.w3.org/2002/07/owl#> .");
                out.printf("<file://%s> a owl:Ontology", main.getAbsolutePath());
                for (FileHandle handle : handles)
                    out.printf(";\n  owl:imports <file://%s> ", handle.file.getAbsolutePath());
                for (String uri : uris)
                    out.printf(";\n  owl:imports %s ", RDFUtils.toNT(new StdURI(uri)));
                out.println(".");
            }
            return mgr.loadOntologyFromOntologyDocument(main);
        } catch (IOException e) {
            throw new TBoxLoadException("Problem while writing importer ontology.", e);
        } catch (OWLOntologyCreationException e) {
            throw new TBoxLoadException("Failed to load importer ontology.", e);
        } finally {
            handles.forEach(FileHandle::close);
        }
    }
}
