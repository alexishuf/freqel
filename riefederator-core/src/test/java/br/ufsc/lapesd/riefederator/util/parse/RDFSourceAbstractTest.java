package br.ufsc.lapesd.riefederator.util.parse;

import br.ufsc.lapesd.riefederator.util.HDTUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFFormatVariant;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.TripleString;
import org.testng.annotations.AfterClass;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;

import static org.testng.Assert.fail;

public class RDFSourceAbstractTest {
    private @Nonnull List<File> tempFiles = new ArrayList<>();
    private @Nonnull List<HDT> hdtObjects = new ArrayList<>();

    @AfterClass(groups = {"fast"})
    public void afterClass() throws IOException {
        for (HDT hdt : hdtObjects)
            hdt.close();
        hdtObjects.clear();
        for (File file : tempFiles) {
            if (!file.delete())
                fail("Failed to delete "+file.getAbsolutePath());
        }
        tempFiles.clear();
    }

    protected @Nonnull File createTempFile() throws IOException {
        return createTempFile("");
    }

    protected @Nonnull File createTempFile(@Nonnull String suffix) throws IOException {
        File file = Files.createTempFile("riefederator", suffix).toFile();
        file.deleteOnExit();
        tempFiles.add(file);
        return file;
    }

    protected @Nonnull Model loadModel(@Nonnull String resourceName) {
        Model m = ModelFactory.createDefaultModel();
        try (InputStream in = getClass().getResourceAsStream(resourceName)) {
            RDFDataMgr.read(m, in, Lang.TTL);
        } catch (IOException e) {
            fail("Unexpected IOException", e);
        }
        return m;
    }

    protected @Nonnull HDT createHDT(@Nonnull Model data) throws Exception {
        StmtIterator it = data.listStatements();
        HDT hdt = HDTManager.generateHDT(new Iterator<TripleString>() {
            @Override public boolean hasNext() {
                return it.hasNext();
            }

            @Override public TripleString next() {
                if (!hasNext()) throw new NoSuchElementException();
                return HDTUtils.toTripleString(it.next());
            }
        }, "uri:relative:", new HDTSpecification(), HDTUtils.NULL_LISTENER);
        hdtObjects.add(hdt);
        return hdt;
    }
    protected @Nonnull HDT saveHDT(@Nonnull File dst, @Nonnull HDT inMemory) throws IOException {
        inMemory.saveToHDT(dst.getAbsolutePath(), HDTUtils.NULL_LISTENER);
        HDT hdt = HDTManager.mapHDT(dst.getAbsolutePath(), HDTUtils.NULL_LISTENER);
        hdtObjects.add(hdt);
        return hdt;
    }
    protected @Nonnull HDT saveIndexedHDT(@Nonnull File dst,
                                          @Nonnull HDT inMemory) throws IOException {
        inMemory.saveToHDT(dst.getAbsolutePath(), HDTUtils.NULL_LISTENER);
        HDT hdt = HDTManager.mapIndexedHDT(dst.getAbsolutePath(), HDTUtils.NULL_LISTENER);
        hdtObjects.add(hdt);
        return hdt;
    }

    protected @Nonnull File extract(@Nonnull String resourceName,
                                    @Nonnull RDFFormat format)  {
        return extract(resourceName, format, "");
    }

    protected @Nonnull File extractSuffixed(@Nonnull String resourceName,
                                            @Nonnull RDFFormat format)  {
        return extract(resourceName, format, format.getLang().getFileExtensions().get(0));
    }

    protected @Nonnull File extract(@Nonnull String resourceName, @Nonnull RDFFormat format,
                                    @Nonnull String suffix)  {
        try {
            File file = createTempFile(suffix);
            try (FileOutputStream out = new FileOutputStream(file)) {
                Model m = loadModel(resourceName);
                RDFDataMgr.write(out, m, format);
            }
            return file;
        } catch (IOException e) {
            throw new AssertionError("Exception extracting to temp file", e); //unreachable
        }
    }

    protected @Nonnull RDFFormat toFormat(@Nonnull Lang lang) {
        RDFFormat best = null;
        for (Field field : RDFFormat.class.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (!Modifier.isStatic(modifiers) || !Modifier.isPublic(modifiers))
                 continue;
            Object value;
            try {
                value = field.get(null);
            } catch (IllegalAccessException e) {
                continue;
            }
            if (value instanceof RDFFormat) {
                RDFFormat format = (RDFFormat) value;
                if (format.getLang().equals(lang)) {
                    RDFFormatVariant v = format.getVariant();
                    if (best == null)                         best = format;
                    else if (v == null)                       best = format;
                    else if (v.equals(RDFFormat.PRETTY))      best = format;
                    else if (v.equals(RDFFormat.UTF8))        best = format;
                    else if (format.equals(RDFFormat.JSONLD)) best = format;
                }
            }
        }
        if (best == null)
            throw new IllegalArgumentException("No RDFFormat for "+lang);
        return best;
    }

    protected @Nonnull Stream<RDFFormat> allFormats() {
        return Arrays.stream(RDFSyntax.values()).map(RDFSyntax::asJenaLang)
                     .filter(Objects::nonNull).map(this::toFormat);
    }
}
