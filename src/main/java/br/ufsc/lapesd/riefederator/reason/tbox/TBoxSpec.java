package br.ufsc.lapesd.riefederator.reason.tbox;

import br.ufsc.lapesd.riefederator.jena.ModelUtils;
import br.ufsc.lapesd.riefederator.jena.TBoxLoader;
import br.ufsc.lapesd.riefederator.model.RDFUtils;
import br.ufsc.lapesd.riefederator.model.term.std.StdURI;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.jena.riot.RDFLanguages.filenameToLang;

public class TBoxSpec implements AutoCloseable {
    private static Logger logger = LoggerFactory.getLogger(TBoxSpec.class);

    private List<String> uris = new ArrayList<>();
    private List<File> files = new ArrayList<>();
    private List<Model> models = new ArrayList<>();
    private List<ImmutablePair<InputStream, Lang>> streams = new ArrayList<>();
    boolean fetchOwlImports = true;

    @CanIgnoreReturnValue
    public @Nonnull TBoxSpec fetchOwlImports(boolean value) {
        fetchOwlImports = value;
        return this;
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
        streams.add(ImmutablePair.of(stream, lang));
        return this;
    }
    @CanIgnoreReturnValue
    public @Nonnull TBoxSpec addStream(@Nonnull InputStream stream) {
        streams.add(ImmutablePair.of(stream, null));
        return this;
    }

    @Override
    public void close() {
        for (ImmutablePair<InputStream, Lang> p : streams) {
            try {
                p.left.close();
            } catch (IOException e) {
                logger.error("Exception when closing InputStream in TBoxSpec", e);
            }
        }
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
        for (ImmutablePair<InputStream, Lang> pair : streams) {
            if (pair.right == null)
                throw new TBoxLoadException("No Lang for stream "+ pair.left);
            loader.addInputStream(pair.left, pair.right);
        }

        return loader.getModel();
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

        for (ImmutablePair<InputStream, Lang> p : streams) {
            String suffix = "." + p.right.getFileExtensions().get(0);
            File temp = null;
            try {
                temp = File.createTempFile("stream", suffix);
                temp.deleteOnExit();
                try (FileOutputStream out = new FileOutputStream(temp)) {
                    IOUtils.copy(p.left, out);
                }
            } catch (IOException e) {
                if (temp == null)
                    throw new TBoxLoadException("Failed to create tem file for stream"+p.left);
                throw new TBoxLoadException("Failed to copy stream "+p.left+" to file "+temp);
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

    public @Nonnull OWLOntology loadOWLOntology(@Nonnull OWLOntologyManager mgr) {
        List<FileHandle> handles = Collections.emptyList();
        try {
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

//    private @Nonnull OWLOntology accOntology(@Nullable OWLOntology acc, @Nonnull OWLOntology onto) {
//        if (acc == null) return onto;
//        onto.axioms().forEach(acc::addAxiom);
//        return acc;
//    }

//    public @Nonnull OWLOntology loadOWLOntology_(OWLOntologyManager mgr) {
//        OWLOntology acc = null;
//        for (Model model : models) {
//            String s = "Model@" + System.identityHashCode(model);
//            File file = null;
//            try {
//                file = toTemp(model);
//                acc = accOntology(acc, mgr.loadOntologyFromOntologyDocument(file));
//            } catch (OWLOntologyCreationException e) {
//                throw new TBoxLoadException("Failed to load Jena "+s+" as TTL in owlapi", e);
//            } finally {
//                if (file != null) {
//                    if (!file.delete())
//                        logger.error("Failed to delete temp file " + file);
//                }
//            }
//        }
//        for (File file : files) {
//            try {
//                acc = accOntology(acc, mgr.loadOntologyFromOntologyDocument(file));
//            } catch (OWLOntologyCreationException e) {
//                throw new TBoxLoadException("Failed to load "+file+" using owlapi", e);
//            }
//        }
//        for (ImmutablePair<InputStream, Lang> pair : streams) {
//            try {
//                acc = accOntology(acc, mgr.loadOntologyFromOntologyDocument(pair.left));
//            } catch (OWLOntologyCreationException e) {
//                throw new TBoxLoadException("Failed to load InputStream using owlapi", e);
//            }
//        }
//        for (String uri : uris) {
//            try {
//                acc = accOntology(acc, mgr.loadOntologyFromOntologyDocument(IRI.create(uri)));
//            } catch (OWLOntologyCreationException e) {
//                throw new TBoxLoadException("Failed to load URI "+uri, e);
//            }
//        }
//        if (acc == null)
//            throw new TBoxLoadException("No sources in spec");
//        return acc;
//    }

}
