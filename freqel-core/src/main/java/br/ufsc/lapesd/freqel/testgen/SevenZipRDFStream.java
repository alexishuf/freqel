package br.ufsc.lapesd.freqel.testgen;

import com.google.common.base.Stopwatch;
import net.sf.sevenzipjbinding.*;
import net.sf.sevenzipjbinding.impl.RandomAccessFileInStream;
import net.sf.sevenzipjbinding.impl.RandomAccessFileOutStream;
import net.sf.sevenzipjbinding.simple.ISimpleInArchiveItem;
import org.apache.commons.io.FileUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.StreamRDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.file.Files;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

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

    public static class TempDir implements Closeable {
        private File dir;

        public TempDir(@Nonnull File parent) throws IOException {
            this.dir = Files.createTempDirectory(parent.toPath(), "freqel").toFile();
        }

        public @Nonnull Collection<File> listAllFiles() {
            return FileUtils.listFiles(dir, null, true);
        }

        public @Nonnull File getDir() {
            return dir;
        }

        public @Nonnull File getFile(@Nonnull String path) {
            return new File(dir, path);
        }

        public void parallelStreamAll(@Nonnull StreamRDF sink) throws IOException {
            IOException[] ex = {null};
            listAllFiles().parallelStream().forEach(file -> {
                try {
                    streamFile(sink, file);
                } catch (IOException e) {
                    if (ex[0] == null) ex[0] = e;
                    else               ex[0].addSuppressed(e);
                }
            });
            if (ex[0] != null)
                throw ex[0];
        }
        public void streamAll(@Nonnull StreamRDF sink) throws IOException {
            for (File file : listAllFiles())
                streamFile(sink, file);

        }

        private void streamFile(@Nonnull StreamRDF sink, File file) throws IOException {
            Matcher matcher = EXT_RX.matcher(file.getName());
            if (!matcher.matches()) {
                logger.info("Skipping file {} since it has no extension", file);
                return;
            }
            Lang lang = RDFLanguages.fileExtToLang(matcher.group(1));
            if (lang == null) {
                logger.info("Skipping {}: extension {} unknown", file, matcher.group(1));
                return;
            }
            try {
                stream(file, lang, sink);
            } catch (RiotException e) {
                logger.error("Bad file: {}: {}", file, e.getMessage());
                throw e;
            }
        }

        @Override public @Nonnull String toString() {
            return dir.getAbsolutePath();
        }

        @Override public void close() throws IOException {
            FileUtils.deleteDirectory(dir);
        }
    }

    public @Nonnull TempDir extractAll() throws IOException {
        initLib();
        TempDir out = new TempDir(file.getParentFile());
        try (RandomAccessFile raf = new RandomAccessFile(file, "r");
             RandomAccessFileInStream rafIn = new RandomAccessFileInStream(raf);
             IInArchive ar = SevenZip.openInArchive(null, rafIn)) {
            ar.extract(null, false, new IArchiveExtractCallback() {
                private long total = 1, complete = 0;
                private String path;

                @Override
                public ISequentialOutStream getStream(int index, ExtractAskMode extractAskMode)
                        throws SevenZipException {
                    this.path = ar.getProperty(index, PropID.PATH).toString();
                    if ((Boolean)ar.getProperty(index, PropID.IS_FOLDER)) {
                        File dst = out.getFile(path);
                        if (!dst.exists() && !dst.mkdirs())
                            throw new SevenZipException("Failed to create dir "+path+" in "+out);
                        return null;
                    } else {
                        File dst = out.getFile(path);
                        File parent = dst.getParentFile();
                        if (!parent.exists() && !parent.mkdirs())
                            throw new SevenZipException("Failed to mkdir "+parent);
                        if (parent.exists() && !parent.isDirectory())
                            throw new SevenZipException(parent+" already exists as non-dir.");
                        try {
                            RandomAccessFile dstRAF = new RandomAccessFile(dst, "rw");
                            return new RandomAccessFileOutStream(dstRAF);
                        } catch (FileNotFoundException e) {
                            throw new SevenZipException("Failed to create file "+dst+
                                                        ": "+e.getMessage(), e);
                        }
                    }
                }

                @Override
                public void prepareOperation(ExtractAskMode extractAskMode) { }

                @Override
                public void setOperationResult(ExtractOperationResult result) throws SevenZipException {
                    logger.info("{} :: {} :: {} [{}% complete]",
                                file, result, path, format("%5.2f", 100*complete/(double)total));
                    if (result != ExtractOperationResult.OK)
                        throw new SevenZipException("Failed to extract "+path+": "+result);
                }

                @Override public void setTotal(long total) {
                    this.total = total;
                }

                @Override public void setCompleted(long complete) {
                    this.complete = complete;
                }
            });
        }
        return out;
    }

    public void streamAll(@Nonnull StreamRDF... sinks) throws IOException {
        initLib();
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
                        "freqel", ".tmp").toFile();
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
                    for (StreamRDF sink : sinks)
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

    private void initLib() {
        if (!SevenZip.isInitializedSuccessfully()) {
            try {
                SevenZip.initSevenZipFromPlatformJAR();
            } catch (SevenZipNativeInitializationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void stream(@Nonnull File temp, @Nonnull Lang lang,
                               @Nonnull StreamRDF sink) throws IOException, RiotException {
        if (!temp.exists())
            throw new IllegalArgumentException(temp+" does not exist");
        if (!temp.isFile())
            throw new IllegalArgumentException(temp+" is not a file");
        if (lang.equals(RDFLanguages.NTRIPLES) || lang.equals(RDFLanguages.N3))
            lang = RDFLanguages.TURTLE; //superset, allows @prefix
        String oldLimit = System.getProperty("jdk.xml.entityExpansionLimit");
        if (oldLimit == null)
            oldLimit = "64000";
        try (FileInputStream inputStream = new FileInputStream(temp)) {
            System.setProperty("jdk.xml.entityExpansionLimit", "256000000");
            RDFDataMgr.parse(sink, inputStream, temp.toURI().toString(), lang);
        } catch (InterruptFileException ignored) {
            /* not an error -- return succesfully */
        } finally {
            System.setProperty("jdk.xml.entityExpansionLimit", oldLimit);
        }
    }
}
