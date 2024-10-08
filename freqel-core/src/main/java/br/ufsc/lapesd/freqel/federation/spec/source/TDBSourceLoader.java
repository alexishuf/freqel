package br.ufsc.lapesd.freqel.federation.spec.source;

import br.ufsc.lapesd.freqel.jena.query.ARQEndpoint;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.BackoffStrategy;
import br.ufsc.lapesd.freqel.util.DictTree;
import com.google.common.collect.Sets;
import org.apache.jena.query.Dataset;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb2.TDB2Factory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.Set;

import static java.util.Collections.singleton;

public class TDBSourceLoader implements SourceLoader {
    private final Set<String> NAMES = Sets.newHashSet("tdb", "tdb2");

    @Override
    public @Nonnull Set<String> names() {
        return NAMES;
    }

    @Override public void setTempDir(@Nonnull File ignored) { }

    @Override public void setSourceCache(@Nullable SourceCache ignored) { }

    @Override public void setIndexingBackoffStrategy(@Nonnull BackoffStrategy ignored) { }

    @Override
    public @Nonnull Set<TPEndpoint> load(@Nonnull DictTree spec,
                                         @Nonnull File reference) throws SourceLoadException {
        String loader = spec.getString("loader", "").trim().toLowerCase();
        String dirPath = spec.getString("dir", null);
        if (dirPath == null)
            throw new SourceLoadException("Missing dir property pointing to TDB directory", spec);
        File childDir = new File(dirPath);
        File dir = childDir.isAbsolute() ? childDir : new File(reference, childDir.getPath());
        String absolutePath = dir.getAbsolutePath();
        if (!dir.exists())
            throw new SourceLoadException(absolutePath +" does not exist", spec);
        if (!dir.isDirectory())
            throw new SourceLoadException(absolutePath +" is not a directory", spec);
        Dataset dataset;
        if (loader.equals("tdb")) {
            dataset = TDBFactory.createDataset(absolutePath);
        } else if (loader.equals("tdb2")) {
            dataset = TDB2Factory.connectDataset(absolutePath);
        } else {
            throw new IllegalArgumentException("Loader "+loader+" not supported by"+this);
        }

        return singleton(ARQEndpoint.forCloseableDataset(dataset));
    }
}
