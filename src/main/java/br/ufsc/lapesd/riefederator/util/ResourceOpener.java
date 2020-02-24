package br.ufsc.lapesd.riefederator.util;

import javax.annotation.Nonnull;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class ResourceOpener {
    public static @Nonnull
    InputStream getStream(@Nonnull String resourcePath) throws FileNotFoundException {
        ClassLoader thread = Thread.currentThread().getContextClassLoader();
        InputStream stream = thread.getResourceAsStream(resourcePath.replaceAll("^/+", ""));
        if (stream != null)
            return stream;

        ClassLoader system = ClassLoader.getSystemClassLoader();
        stream = system.getResourceAsStream(resourcePath.replaceAll("^/+", ""));
        if (stream != null)
            return stream;

        ClassLoader local = ResourceOpener.class.getClassLoader();
        stream = local.getResourceAsStream(resourcePath);
        if (stream == null)
            throw new FileNotFoundException("Resource not found: " + resourcePath);
        return stream;
    }

    public static @Nonnull InputStream getStream(@Nonnull Class<?> cls,
                                                 @Nonnull String path) throws FileNotFoundException{
        InputStream stream = cls.getResourceAsStream(path);
        if (stream != null)
            return stream;

        stream = cls.getClassLoader().getResourceAsStream(path);
        if (stream != null)
            return stream;

        try {
            String absPath = cls.getName().replaceAll("\\.[^.]*$", "")
                    .replace('.', '/')
                    + "/" + path.replaceAll("^/+", "");
            return getStream(absPath);
        } catch (FileNotFoundException ignored) {}

        try {
            return getStream(path);
        } catch (FileNotFoundException ignored) {}

        throw new FileNotFoundException("Could not find resource " + path + " relative to " + cls);
    }
}
