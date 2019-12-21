package br.ufsc.lapesd.riefederator.jena;

import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
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
