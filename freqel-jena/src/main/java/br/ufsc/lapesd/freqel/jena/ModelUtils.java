package br.ufsc.lapesd.freqel.jena;

import com.google.common.base.Preconditions;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;

import static java.util.Spliterator.*;
import static java.util.Spliterators.spliteratorUnknownSize;

public class ModelUtils {
    public static @Nonnull <T> Stream<T> toStream(Iterator<T> it) {
        Spliterator<T> spl = spliteratorUnknownSize(it, DISTINCT | NONNULL);
        return StreamSupport.stream(spl, false);
    }

    public static @Nonnull  Stream<Statement> list(@Nonnull Model model, @Nullable Resource s,
                                                   @Nullable Property p, @Nullable Object o) {
        return list(model, s, p, o, Function.identity());
    }

    public static @Nonnull <R> Stream<R> list(@Nonnull Model model, @Nullable Resource s,
                                              @Nullable Property p, @Nullable Object o,
                                              @Nonnull Function<Statement, R> project) {
        Stream<Statement> ss;
        if (o == null || o instanceof RDFNode)
            ss = toStream(model.listStatements(s, p, (RDFNode) o));
        else
            ss = toStream(model.listStatements(s, p, o.toString()));
        return ss.map(project);
    }

    private static @Nonnull Stream<Resource>
    listResources(@Nonnull Model model, @Nullable Resource s,
                  @Nullable Property p, @Nullable Object o) {
        Preconditions.checkArgument(((s == null) ^ (p == null)) ^ (o == null),
                "Exactly one among s, p and o must be null");
        Function<Statement, Resource> prj;
        if      (s == null) prj = Statement::getSubject;
        else if (p == null) prj = Statement::getPredicate;
        else                prj = t -> t.getObject().isResource() ? t.getResource() : null;
        return list(model, s, p, o, prj).filter(Objects::nonNull);
    }

    public static  @Nonnull Resource inModel(@Nonnull Model model, @Nonnull Resource resource) {
        if (resource.getModel() == model) return resource;
        else if (resource.isAnon())       return model.createResource(resource.getId());
        else                              return model.createResource(resource.getURI());
    }

    public static @Nonnull Stream<Resource>
    closure(@Nonnull Model model, @Nonnull Resource start, @Nonnull Property prop,
            boolean fromObject, boolean strict) {
        start = inModel(model, start);
        Set<Resource> visited = new HashSet<>();
        ArrayDeque<Resource> queue = new ArrayDeque<>();
        if (strict) {
            visited.add(start);
            if (fromObject) listResources(model, null , prop, start).forEach(queue::add);
            else            listResources(model, start, prop, null ).forEach(queue::add);
        } else {
            queue.add(start);
        }

        Spliterator<Resource> split = spliteratorUnknownSize(new Iterator<Resource>() {
            private Resource current = null;
            @Override
            public boolean hasNext() {
                if (current != null) return true;
                while (!queue.isEmpty()) {
                    Resource r = queue.remove();
                    if (visited.add(r)) {
                        if (fromObject) listResources(model, null, prop, r   ).forEach(queue::add);
                        else            listResources(model, r   , prop, null).forEach(queue::add);
                        current = r;
                        return true;
                    }
                }
                return false;
            }
            @Override
            public @Nonnull Resource next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                Resource old = this.current;
                current = null;
                return old;
            }
        }, DISTINCT | NONNULL | IMMUTABLE);
        return StreamSupport.stream(split, false);
    }

    public static File toTemp(@Nonnull Model model, boolean gzip) throws IOException {
        return toTemp(model, gzip, RDFFormat.NT, true);
    }

    public static File toTemp(@Nonnull Model model, boolean gzip,
                              @Nonnull RDFFormat format, boolean deleteOnExit) throws IOException {
        String suffix = "." + format.getLang().getFileExtensions().get(0) + (gzip ? ".gz" : "");
        File file = File.createTempFile("model", suffix);
        if (deleteOnExit)
            file.deleteOnExit();
        OutputStream out = new FileOutputStream(file);
        try {
            if (gzip) out = new GZIPOutputStream(out);
            RDFDataMgr.write(out, model, format);
        } finally {
            out.flush();
            out.close();
        }
        return file;
    }
}
