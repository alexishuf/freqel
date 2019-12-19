package br.ufsc.lapesd.riefederator.reason.tbox;

import br.ufsc.lapesd.riefederator.jena.ModelUtils;
import br.ufsc.lapesd.riefederator.jena.TBoxLoader;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.apache.jena.riot.RDFLanguages.filenameToLang;

public class TBoxSpec {
    private static Logger logger = LoggerFactory.getLogger(TBoxSpec.class);

    private List<String> uris = new ArrayList<>();
    private List<File> files = new ArrayList<>();
    private List<Model> models = new ArrayList<>();
    private List<ImmutablePair<InputStream, Lang>> streams = new ArrayList<>();
    boolean fetchOwlImports = true;

    public @Nonnull TBoxSpec fetchOwlImports(boolean value) {
        fetchOwlImports = value;
        return this;
    }

    public @Nonnull TBoxSpec addURI(@Nonnull String uri) {
        uris.add(uri);
        return this;
    }

    public @Nonnull TBoxSpec addFile(@Nonnull File file) {
        files.add(file);
        return this;
    }

    public @Nonnull TBoxSpec addModel(@Nonnull Model model) {
        models.add(model);
        return this;
    }

    public @Nonnull TBoxSpec addStreams(@Nonnull InputStream stream, @Nonnull Lang lang) {
        streams.add(ImmutablePair.of(stream, lang));
        return this;
    }
    public @Nonnull TBoxSpec addStreams(@Nonnull InputStream stream) {
        streams.add(ImmutablePair.of(stream, null));
        return this;
    }

    public @Nonnull Model loadToModel() throws TBoxLoadException {
        Model model = ModelFactory.createDefaultModel();
        models.forEach(model::add);
        for (File file : files) {
            String base = "file://" + file.getAbsolutePath();
            try (FileInputStream in = new FileInputStream(file)) {
                RDFDataMgr.read(model, in, base, filenameToLang(file.getName(), Lang.TTL));
            } catch (Exception e) {
                throw new TBoxLoadException("Failed to load file "+file, e);
            }
        }
        for (ImmutablePair<InputStream, Lang> pair : streams) {
            if (pair.right == null)
                throw new TBoxLoadException("No Lang for stream "+ pair.left);
            String base = "inputstream://" + System.identityHashCode(pair.left);
            RDFDataMgr.read(model, pair.left, base, pair.right);
        }
        for (String uri : uris) {
            try {
                RDFDataMgr.read(model, uri);
            } catch (RuntimeException e) {
                throw new TBoxLoadException("Failed to load URI "+uri, e);
            }
        }
        return fetchOwlImports ? new TBoxLoader().fetchingImports(true).setModel(model).getModel()
                               : model;
    }

    public @Nonnull OWLOntology loadOWLOntology() {
        return loadOWLOntology(OWLManager.createOWLOntologyManager());
    }
    private @Nonnull OWLOntology accOntology(@Nullable OWLOntology acc, @Nonnull OWLOntology onto) {
        if (acc == null) return onto;
        onto.axioms().forEach(acc::addAxiom);
        return acc;
    }

    public @Nonnull OWLOntology loadOWLOntology(OWLOntologyManager mgr) {
        OWLOntology acc = null;
        for (Model model : models) {
            String s = "Model@" + System.identityHashCode(model);
            File file = null;
            try {
                file = ModelUtils.toTemp(model, false);
                acc = accOntology(acc, mgr.loadOntologyFromOntologyDocument(file));
            } catch (IOException e) {
                throw new TBoxLoadException("Failed to create temp file for "+s, e);
            } catch (OWLOntologyCreationException e) {
                throw new TBoxLoadException("Failed to load Jena "+s+" as TTL in owlapi", e);
            } finally {
                if (file != null) {
                    if (!file.delete())
                        logger.error("Failed to delete temp file " + file);
                }
            }
        }
        for (File file : files) {
            try {
                acc = accOntology(acc, mgr.loadOntologyFromOntologyDocument(file));
            } catch (OWLOntologyCreationException e) {
                throw new TBoxLoadException("Failed to load "+file+" using owlapi", e);
            }
        }
        for (ImmutablePair<InputStream, Lang> pair : streams) {
            try {
                acc = accOntology(acc, mgr.loadOntologyFromOntologyDocument(pair.left));
            } catch (OWLOntologyCreationException e) {
                throw new TBoxLoadException("Failed to load InputStream using owlapi", e);
            }
        }
        for (String uri : uris) {
            try {
                acc = accOntology(acc, mgr.loadOntologyFromOntologyDocument(IRI.create(uri)));
            } catch (OWLOntologyCreationException e) {
                throw new TBoxLoadException("Failed to load URI "+uri, e);
            }
        }
        if (acc == null)
            throw new TBoxLoadException("No sources in spec");
        return acc;
    }

}
