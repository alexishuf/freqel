package br.ufsc.lapesd.freqel.federation.inject.dagger.modules;

import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoadException;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoader;
import br.ufsc.lapesd.freqel.federation.spec.source.SourceLoaderRegistry;
import br.ufsc.lapesd.freqel.query.endpoint.TPEndpoint;
import br.ufsc.lapesd.freqel.util.DictTree;

import javax.annotation.Nonnull;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.*;

public class ModuleHelper {

    public static @Nonnull TPEndpoint createEndpoint(@Nonnull String loaderName,
                                                     @Nonnull String location,
                                                     @Nonnull SourceLoaderRegistry loaderRegistry,
                                                     @Nonnull File referenceDir) {
        DictTree sourceSpec = new DictTree();
        sourceSpec.put("loader", loaderName);
        sourceSpec.put("location", location);
        try {
            SourceLoader loader = loaderRegistry.getLoaderFor(sourceSpec);
            Set<TPEndpoint> eps = loader.load(sourceSpec, referenceDir);
            Iterator<TPEndpoint> it = eps.iterator();
            TPEndpoint ep = it.next();
            if (it.hasNext())
                throw new IllegalArgumentException("Multiple TPEndpoints spawned from "+location);
            return ep;
        } catch (SourceLoadException e) {
            throw new RuntimeException("Cannot load "+location+": "+e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T get(@Nonnull Class<T> interfaceClass, @Nonnull String className,
                            @Nonnull Object... injectedInstances) {
        for (Object instance : injectedInstances) {
            if (instance.getClass().getName().equals(className))
                return (T)instance;
        }
        List<T> loadedInstances = new ArrayList<>();
        for (T instance : ServiceLoader.load(interfaceClass))
            loadedInstances.add(instance);
        for (T instance : loadedInstances) {
            if (instance.getClass().getName().equals(className))
                return instance;
        }
        Class<?> cls = null;
        try {
            cls = ClassLoader.getSystemClassLoader().loadClass(className);
            Constructor<?> ct = cls.getConstructor();
            Object instance = ct.newInstance();
            if (!interfaceClass.isAssignableFrom(instance.getClass())) {
                throw new IllegalArgumentException(interfaceClass+" cannot be assigned from "+
                                                   instance.getClass());
            }
            return (T)instance;
        } catch (ClassNotFoundException e) {
            // maybe className is abbreviated
            for (Object instance : injectedInstances) {
                if (instance.getClass().getName().endsWith(className))
                    return (T)instance;
            }
            for (T instance : loadedInstances) {
                if (instance.getClass().getName().equals(className))
                    return instance;
            }
            throw new IllegalArgumentException("There is no "+className+" in the classpath", e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(cls+" has no default constructor");
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Could not instantiate "+cls+": "+e.getMessage(), e);
        }
    }
}
