package br.ufsc.lapesd.riefederator.util;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExtractedResources implements AutoCloseable {
    protected final @Nonnull List<ExtractedResource> list;

    public ExtractedResources(Collection<ExtractedResource> collection) {
        this.list = new ArrayList<>(collection);
    }

    public @Nonnull Stream<File> stream() {
        return list.stream().map(ExtractedResource::getFile);
    }

    public @Nonnull List<File> getFiles() {
        return stream().collect(Collectors.toList());
    }

    @Override
    public void close() {
        list.forEach(ExtractedResource::close);
    }
}
