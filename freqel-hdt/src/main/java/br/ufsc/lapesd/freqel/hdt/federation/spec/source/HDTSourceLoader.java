package br.ufsc.lapesd.freqel.hdt.federation.spec.source;

import br.ufsc.lapesd.freqel.federation.spec.source.SourceCache;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoadException;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoader;
import br.ufsc.lapesd.freqel.hdt.query.HDTEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.DictTree;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

public class HDTSourceLoader implements SourceLoader {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(HDTSourceLoader.class);
    private static final @Nonnull Set<String> NAMES = Collections.singleton("hdt");
    private static final @Nonnull Pattern URI_RX = Pattern.compile("^[^:]+:");
    private @Nonnull File tempDir = new File(System.getProperty("java.io.tmpdir"));
    private @Nullable SourceCache sourceCache;


    @Override public @Nonnull Set<String> names() {
        return NAMES;
    }

    @Override public void setTempDir(@Nonnull File tempDir) {
        this.tempDir = tempDir;
    }

    @Override public void setSourceCache(@Nullable SourceCache sourceCache) {
        this.sourceCache = sourceCache;
    }

    private @Nonnull File createTempFile() throws IOException {
        if (!tempDir.exists() && !tempDir.mkdirs())
            throw new IOException("Could not mkdir -p temp dir "+tempDir);
        return Files.createTempFile(tempDir.toPath(), "freqel", ".hdt").toFile();
    }

    @Override
    public @Nonnull Set<TPEndpoint> load(@Nonnull DictTree spec,
                                         @Nonnull File referenceDir) throws SourceLoadException {
        File file = getFile(spec, referenceDir);
        try {
            return Collections.singleton(HDTEndpoint.fromFile(file));
        } catch (IOException e) {
            throw new SourceLoadException("Problem mapping "+file+": "+e.getMessage(), e, spec);
        }
    }

    private @Nonnull File getFile(DictTree s, @Nonnull File reference) throws SourceLoadException {
        String path = s.getString("file");
        if (path != null)
            return new File(reference, path);
        path = s.getString("location", s.getString("uri", s.getString("uri")));
        if (path == null)
            throw new SourceLoadException("No file/location/uri/url entry!", s);
        if (URI_RX.matcher(path).find() && !path.startsWith("file:")) {
            File outFile = null;
            try {
                outFile = createTempFile();
                URL url = new URL(path);
                try (InputStream in = url.openStream();
                     OutputStream out = new FileOutputStream(outFile)) {
                    IOUtils.copy(in, out);
                } catch (IOException e) {
                    if (outFile.exists() && !outFile.delete())
                        logger.error("Failed to delete temp file {}", outFile);
                    throw e;
                }
            } catch (MalformedURLException e) {
                throw new SourceLoadException("Malformed URL "+path, e, s);
            } catch (IOException e) {
                throw new SourceLoadException("IOException during download of "+path+
                                              " to "+outFile, e, s);
            }
            path = outFile.getAbsolutePath();
        }
        return new File(path);
    }
}
