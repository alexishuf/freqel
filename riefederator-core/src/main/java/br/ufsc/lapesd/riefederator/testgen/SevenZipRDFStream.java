package br.ufsc.lapesd.riefederator.testgen;

import com.google.common.base.Stopwatch;
import net.sf.sevenzipjbinding.ExtractOperationResult;
import net.sf.sevenzipjbinding.IInArchive;
import net.sf.sevenzipjbinding.SevenZip;
import net.sf.sevenzipjbinding.SevenZipNativeInitializationException;
import net.sf.sevenzipjbinding.impl.RandomAccessFileInStream;
import net.sf.sevenzipjbinding.impl.RandomAccessFileOutStream;
import net.sf.sevenzipjbinding.simple.ISimpleInArchiveItem;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.StreamRDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SevenZipRDFStream {
    private static final Logger logger = LoggerFactory.getLogger(SevenZipRDFStream.class);
    private static final Pattern EXT_RX = Pattern.compile("^.*\\.([^.]*)$");

    private final File file;
    private long timeoutSeconds = Long.MAX_VALUE;

    public static final class InterruptFileException extends RuntimeException {

    }

    public SevenZipRDFStream(@Nonnull File file) {
        this.file = file;
    }

    public boolean exists() {
        return this.file.exists();
    }

    public @Nonnull SevenZipRDFStream setTimeout(long value, @Nonnull TimeUnit unit) {
        timeoutSeconds = TimeUnit.SECONDS.convert(value, unit);
        return this;
    }

    public void streamAll(@Nonnull StreamRDF sink) throws IOException {
        if (!SevenZip.isInitializedSuccessfully()) {
            try {
                SevenZip.initSevenZipFromPlatformJAR();
            } catch (SevenZipNativeInitializationException e) {
                throw new RuntimeException(e);
            }
        }
        File parent = file.getParentFile();
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        Stopwatch sw = Stopwatch.createStarted();
        try (RandomAccessFileInStream rafStream = new RandomAccessFileInStream(raf);
             IInArchive ar = SevenZip.openInArchive(null, rafStream)) {
            for (ISimpleInArchiveItem item : ar.getSimpleInterface().getArchiveItems()) {
                if (item.isFolder()) continue;

                Matcher matcher = EXT_RX.matcher(item.getPath());
                if (!matcher.matches()) {
                    logger.info("Skipping entry {} of archive {} since it has no extension",
                                item.getPath(), file.getAbsoluteFile());
                    continue;
                }
                Lang lang = RDFLanguages.fileExtToLang(matcher.group(1));
                if (lang == null) {
                    logger.info("Skipping entry {} of archive {} since it is not an RDF file",
                                item.getPath(), file.getAbsolutePath());
                    continue;
                }
                File temp = Files.createTempFile(parent.toPath(),
                        "riefederator", ".tmp").toFile();
                temp.deleteOnExit();
                RandomAccessFile tempRAF = new RandomAccessFile(temp, "rw");
                RandomAccessFileOutStream tempStream = new RandomAccessFileOutStream(tempRAF);
                try {
                    logger.info("Extracting {} from {}...", item.getPath(), file);
                    ExtractOperationResult result = item.extractSlow(tempStream);
                    if (result != ExtractOperationResult.OK) {
                        throw new RuntimeException("Problem extracting entry " + item.getPath() +
                                " from archive " + file.getAbsolutePath() +
                                ": " + result);
                    }
                    logger.info("Parsing {} from {}...", item.getPath(), file);
                    stream(temp, lang, sink);
                } catch (RiotException e) {
                    logger.error("Problem with file {} extracted to {}: {}. Stopped processing.",
                                  item.getPath(), file, e.getMessage());
                } finally {
                    tempStream.close();
                    tempRAF.close();
                    if (temp.exists() && !temp.delete())
                        logger.warn("Failed to delete temp file {}", temp.getAbsolutePath());
                }
                if (sw.elapsed(TimeUnit.SECONDS) > timeoutSeconds) {
                    logger.info("Allowed time elapsed for {}", file);
                    break; //stop
                }
            }
        }
    }

    private void stream(@Nonnull File temp, @Nonnull Lang lang,
                        @Nonnull StreamRDF sink) throws IOException, RiotException {
        if (!temp.exists())
            throw new IllegalArgumentException(temp+" does not exist");
        if (!temp.isFile())
            throw new IllegalArgumentException(temp+" is not a file");
        if (lang.equals(RDFLanguages.NTRIPLES) || lang.equals(RDFLanguages.N3))
            lang = RDFLanguages.TURTLE; //superset, allows @prefix
        try (FileInputStream inputStream = new FileInputStream(temp)) {
            RDFDataMgr.parse(sink, inputStream, temp.toURI().toString(), lang);
        } catch (InterruptFileException ignored) {
            /* not an error -- return succesfully */
        }
    }
}
