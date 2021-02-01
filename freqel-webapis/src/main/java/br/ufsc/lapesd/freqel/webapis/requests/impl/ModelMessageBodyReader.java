package br.ufsc.lapesd.freqel.webapis.requests.impl;

import com.google.errorprone.annotations.concurrent.LazyInit;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFParserRegistry;
import org.apache.jena.shared.JenaException;

import javax.annotation.Nonnull;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Provider
public class ModelMessageBodyReader implements MessageBodyReader<Model> {
    private static @LazyInit Set<String> supportedMediaTypesSet;
    private static @LazyInit String[] supportedMediaTypes;


    public static @Nonnull Set<String> getSupportedMediaTypesSet() {
        if (supportedMediaTypesSet == null) {
            supportedMediaTypesSet = RDFLanguages.getRegisteredLanguages().stream()
                    .filter(l -> !Objects.equals(l, Lang.RDFNULL))
                    .filter(RDFParserRegistry::isTriples)
                    .flatMap(l -> l.getAltContentTypes().stream()).collect(Collectors.toSet());
        }
        return supportedMediaTypesSet;
    }

    public static @Nonnull String[] getSupportedMediaTypes() {
        if (supportedMediaTypes == null)
            supportedMediaTypes = getSupportedMediaTypesSet().toArray(new String[0]);
        return supportedMediaTypes;
    }

    @Override
    public boolean isReadable(Class<?> aClass, Type type, Annotation[] annotations,
                              MediaType mediaType) {
        return Model.class.isAssignableFrom(aClass)
                && getSupportedMediaTypesSet().contains(simplify(mediaType));
    }

    @Override
    public Model readFrom(Class<Model> aClass, Type type, Annotation[] annotations,
                          MediaType mediaType, MultivaluedMap<String, String> multivaluedMap,
                          InputStream inputStream) throws IOException, WebApplicationException {
        Lang lang = toLangOrThrow(mediaType);
        try {
            Model model = ModelFactory.createDefaultModel();
            RDFDataMgr.read(model, inputStream, lang);
            return model;
        } catch (JenaException e) {
            if (hasIOExceptionCause(e)) throw new IOException(e);
            throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }


    public static String simplify(MediaType mediaType) {
        return mediaType.getType() + "/" + mediaType.getSubtype();
    }

    public static Lang toLangOrThrow(MediaType mediaType) {
        String simplified = simplify(mediaType);
        Lang lang = RDFLanguages.contentTypeToLang(simplified);
        if (lang == null) {
            throw new WebApplicationException("There is no Jena Lang registered for " + simplified,
                    Response.Status.INTERNAL_SERVER_ERROR);
        }
        return lang;
    }

    public static boolean hasIOExceptionCause(Exception e) {
        for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
            if (cause instanceof IOException) return true;
        }
        return false;
    }
}
