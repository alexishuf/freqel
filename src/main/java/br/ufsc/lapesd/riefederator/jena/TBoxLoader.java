package br.ufsc.lapesd.riefederator.jena;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static br.ufsc.lapesd.riefederator.jena.ModelUtils.list;

public class TBoxLoader {
    private boolean fetchImports = true;
    private final @Nonnull Set<Resource> fetched = new HashSet<>();
    private final @Nonnull Model model;
    private final @Nullable String name;

    /* --- configurations --- */

    public boolean getFetchImports() {
        return fetchImports;
    }
    public TBoxLoader fetchingImports(boolean fetchImports) {
        this.fetchImports = fetchImports;
        return this;
    }

    /* --- general --- */

    public TBoxLoader(@Nullable String name) {
        this.model = ModelFactory.createDefaultModel();
        this.name = name;
    }

    public TBoxLoader() {
        this(null);
    }


    @Override
    public @Nonnull String toString() {
        return name != null ? "["+name+"]" : super.toString();
    }

    public @Nonnull Model getModel() {
        return model;
    }

    /* --- load stuff --- */

    /**
     * Copies the contents of model into the TBox. Ownership of model is not transferred
     */
    @Contract("_ -> this") @CanIgnoreReturnValue
    public @Nonnull TBoxLoader addModel(@Nonnull Model model) {
        list(model, null, RDF.type, OWL2.Ontology, Statement::getSubject).forEach(fetched::add);
        this.model.add(model);
        if (fetchImports) {
            list(model, null, OWL2.imports, null, Statement::getResource)
                    .filter(r -> !fetched.contains(r))
                    .forEach(this::fetchOntology);
        }
        return this;
    }

    @CheckReturnValue
    private @Nullable Model loadFromResource(@Nonnull String path, @Nonnull ClassLoader loader,
                                             @Nonnull Lang lang) {
        Model model = ModelFactory.createDefaultModel();
        try (InputStream stream = loader.getResourceAsStream(path)) {
            if (stream == null) return null;
            RDFDataMgr.read(model, stream, lang);
            return model;
        } catch (IOException e) {
            throw new RuntimeException("IOException reading from resource " + path, e);
        }
    }

    /**
     * Loads RDF into the TBox from a resource file.
     *
     * @param path absolute or relative (to this class) path to the resource path
     * @param loader class loader to try first
     */
    @Contract("_, _ -> this") @CanIgnoreReturnValue
    public @Nonnull TBoxLoader addFromResource(@Nonnull String path,
                               @Nonnull ClassLoader loader) {
        Lang lang = RDFLanguages.filenameToLang(path);
        ClassLoader system = ClassLoader.getSystemClassLoader();
        String pathNoTrailing = path.startsWith("/") ? path.substring(1) : path;

        Model model = loadFromResource(path, loader, lang);
        if (model == null)
            model = loadFromResource(pathNoTrailing, system, lang);
        if (model == null && !path.startsWith("/"))
            model = loadFromResource("br/ufsc/lapesd/riefederator/" + path, system, lang);
        if (model == null && !path.startsWith("/"))
            model = loadFromResource("br/ufsc/lapesd/riefederator/" + path, loader, lang);
        if (model == null && !path.startsWith("/"))
            model = loadFromResource("/br/ufsc/lapesd/riefederator/" + path, loader, lang);
        if (model == null)
            throw new RuntimeException("Resource not found", new FileNotFoundException(path));
        else
            addModel(model);
        return this;
    }

    @Contract("_ -> this") @CanIgnoreReturnValue
    public @Nonnull TBoxLoader addFromResource(@Nonnull String path) {
        return addFromResource(path, getClass().getClassLoader());
    }

    @Contract("-> this") @CanIgnoreReturnValue
    public @Nonnull TBoxLoader addRDF() {
        addFromResource("rdf.ttl");
        return this;
    }

    @Contract("-> this") @CanIgnoreReturnValue
    public @Nonnull TBoxLoader addRDFS() {
        addFromResource("rdf-schema.ttl");
        return this;
    }

    @Contract("-> this") @CanIgnoreReturnValue
    public @Nonnull TBoxLoader addOWL() {
        addFromResource("owl.ttl");
        return this;
    }

    @Contract("_ -> this") @CanIgnoreReturnValue
    public @Nonnull TBoxLoader fetchOntology(@Nonnull Resource ontology) {
        if (ontology.isAnon()) return this;
        String uri = ontology.getURI();
        if (!uri.endsWith("#"))
            uri += "#";

        if (uri.equals(RDF.getURI()))
            return addRDF();
        else if(uri.equals(RDFS.getURI()))
            return addRDFS();
        else if (uri.equals(OWL2.getURI()))
            return addOWL();
        else
            return fetchOntology(ontology.getURI());
    }

    @Contract("_ -> this") @CanIgnoreReturnValue
    public @Nonnull TBoxLoader fetchOntology(@Nonnull String uri) {
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, uri);
        addModel(model);
        return this;
    }
}
