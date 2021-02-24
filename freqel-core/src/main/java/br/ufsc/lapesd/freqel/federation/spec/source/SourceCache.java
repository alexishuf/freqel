package br.ufsc.lapesd.freqel.federation.spec.source;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.esotericsoftware.yamlbeans.YamlWriter;
import com.google.errorprone.annotations.CanIgnoreReturnValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SourceCache {
    private final  @Nonnull File dir;
    private @Nonnull Index index = new Index(null);

    @SuppressWarnings("unchecked")
    private static class Index {
        private final Map<String, Object> map;

        public Index(@Nullable Map<String, Object> map) {
            this.map = map;
        }

        public @Nullable String getFile(@Nonnull String endpointId, @Nonnull String fileType) {
            if (map == null)
                return null;
            Map<String, String> type2file = (Map<String, String>)map.get(endpointId);
            return type2file == null ? null : type2file.get(fileType);
        }

        public void setFile(@Nonnull String endpointId, @Nonnull String fileType, @Nonnull String path) {
            if (map != null) {
                Map<String, String> type2file;
                type2file = (Map<String, String>) map.computeIfAbsent(endpointId, k -> new HashMap<>());
                type2file.put(fileType, path);
            }
        }

        public void write(@Nonnull Writer writer) throws YamlException {
            YamlWriter w = new YamlWriter(writer);
            w.write(map);
            w.close();
        }
        public void write(@Nonnull OutputStream stream) throws IOException {
            try (OutputStreamWriter writer = new OutputStreamWriter(stream, UTF_8)) {
                write(writer);
            }
        }
        public void write(@Nonnull File file) throws IOException {
            try (OutputStream out = new FileOutputStream(file)) {
                write(out);
            }
        }
    }

    @Inject public SourceCache(@Named("sourceCacheDir") @Nonnull File dir) {
        this.dir = dir;
    }

    /**
     * Loads the index from disk, but only if not yet loaded
     */
    @CanIgnoreReturnValue
    public synchronized @Nonnull SourceCache loadIndex() throws IOException {
        if (index.map == null)
            reloadIndex();
        return this;
    }

    /**
     * Loads the index from disk, even if an index is loaded.
     *
     * Warning: this discards the current index contents in memory
     */
    @CanIgnoreReturnValue
    public synchronized @Nonnull SourceCache reloadIndex() throws IOException {
        File file = new File(dir, "index.yaml");
        if (file.exists()) {
            try (FileInputStream stream = new FileInputStream(file);
                 InputStreamReader reader = new InputStreamReader(stream, UTF_8)) {
                YamlReader yamlReader = new YamlReader(reader);
                for (Object obj = yamlReader.read(); obj != null; obj = yamlReader.read()) {
                    if (obj instanceof Map) {
                        //noinspection unchecked
                        index = new Index((Map<String, Object>)obj);
                        break;
                    }
                }
            }
        } else {
            index = new Index(new HashMap<>());
        }
        return this;
    }

    @CanIgnoreReturnValue
    public synchronized @Nonnull SourceCache saveIndex() throws IOException {
        if (index.map != null)
            index.write(new File(dir, "index.yaml"));
        return this;
    }

    public synchronized @Nullable File getFile(@Nonnull String fileType,
                                  @Nonnull String sourceId) throws IOException {
        loadIndex();
        String path = index.getFile(sourceId, fileType);
        if (path == null)
            return null;

        File file = new File(path);
        if (file.isAbsolute())
            return file;
        return new File(dir, path).getAbsoluteFile();
    }

    public synchronized @Nonnull File createFile(@Nonnull String fileType, @Nonnull String ext,
                                    @Nonnull String sourceIdentifier) throws IOException {
        File file = getFile(fileType, sourceIdentifier);
        if (file != null) return file;

        Path path = Files.createTempFile(dir.toPath(), fileType + "-", "." + ext);
        index.setFile(sourceIdentifier, fileType, dir.toPath().relativize(path).toString());
        saveIndex();
        return path.toAbsolutePath().toFile();
    }

    public @Nonnull File getDir() {
        return dir;
    }
}
