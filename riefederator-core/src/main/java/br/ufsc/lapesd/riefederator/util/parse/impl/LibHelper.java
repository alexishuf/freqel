package br.ufsc.lapesd.riefederator.util.parse.impl;

import br.ufsc.lapesd.riefederator.util.parse.RDFIterationDispatcher;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;

class LibHelper {
    static void registerAll(@Nonnull Class<?> parent, @Nonnull Class<?> iface,
                            @Nonnull RDFIterationDispatcher dispatcher) {
        for (Class<?> cls : parent.getClasses()) {
            if (!iface.isAssignableFrom(cls))
                continue;
            TargetClass ann = cls.getAnnotation(TargetClass.class);
            if (ann != null) {
                try {
                    Object implementation = cls.getConstructor().newInstance();
                    Class<?> dispatcherCls = dispatcher.getClass();
                    Method m = dispatcherCls.getMethod("register", Class.class, iface);
                    m.invoke(dispatcher, ann.value(), implementation);
                } catch (Exception e) {
                    throw new RuntimeException("Unexpected exception", e);
                }
            }
        }
    }
}
