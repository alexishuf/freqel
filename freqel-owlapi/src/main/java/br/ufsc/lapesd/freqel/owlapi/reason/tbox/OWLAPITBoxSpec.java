package br.ufsc.lapesd.freqel.owlapi.reason.tbox;

import br.ufsc.lapesd.freqel.reason.tbox.TBoxLoadException;
import br.ufsc.lapesd.freqel.reason.tbox.TBoxSpec;
import com.github.lapesd.rdfit.RIt;
import com.github.lapesd.rdfit.source.RDFFile;
import com.github.lapesd.rdfit.util.impl.EternalCache;
import com.github.lapesd.rdfit.util.impl.RDFBlob;
import org.apache.jena.riot.writer.NQuadsWriter;
import org.apache.jena.sparql.core.Quad;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.util.SimpleIRIMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.github.lapesd.rdfit.util.Utils.toASCIIString;
import static java.util.stream.Collectors.toList;

public class OWLAPITBoxSpec {
    private static final Logger logger = LoggerFactory.getLogger(OWLAPITBoxSpec.class);
    private final @Nonnull TBoxSpec spec;

    public OWLAPITBoxSpec(@Nonnull TBoxSpec spec) {
        this.spec = spec;
    }

    public @Nonnull TBoxSpec getSpec() {
        return spec;
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
        try (OWLAPIFiles extracted = new OWLAPIFiles(mgr, spec.getSources())) {
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
