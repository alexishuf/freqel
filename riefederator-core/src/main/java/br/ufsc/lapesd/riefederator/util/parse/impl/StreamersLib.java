package br.ufsc.lapesd.riefederator.util.parse.impl;

import br.ufsc.lapesd.riefederator.jena.TBoxLoader;
import br.ufsc.lapesd.riefederator.util.parse.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.system.StreamRDF;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import static br.ufsc.lapesd.riefederator.util.parse.RDFSyntax.HDT;
import static br.ufsc.lapesd.riefederator.util.parse.RDFSyntax.UNKNOWN;

public class StreamersLib {
    private static final Pattern URI_RX = Pattern.compile("(?i)^\\w+:");
    private static final Pattern EXT_RX = Pattern.compile("(?i)\\.([^.]+)$");
    private static final Pattern HDT_RX = Pattern.compile("(?i)\\.hdt$");

    public static void registerAll(@Nonnull RDFIterationDispatcher dispatcher) {
        LibHelper.registerAll(StreamersLib.class, RDFStreamer.class, dispatcher);
    }

    @TargetClass(File.class)
    public static class FileStreamer extends RDFStreamerBase {
        @Override
        public void stream(@Nonnull Object source,
                             @Nonnull StreamRDF streamRDF) throws SourceIterationException {
            File file = (File) source;
            RDFSyntax syntax = getFileSyntax(file);
            doStream(source, streamRDF, file.toURI().toString(), syntax.asJenaLang());
        }

        @Override public boolean canStream(@Nonnull Object source) {
            return source instanceof File && getFileSyntax((File) source) != HDT;
        }
    }

    @TargetClass(URI.class)
    public static class URIStreamer extends RDFStreamerBase {
        private TBoxLoader loader = new TBoxLoader();

        @Override
        public void stream(@Nonnull Object source,
                           @Nonnull StreamRDF streamRDF) throws SourceIterationException {
            URI u = (URI) source;
            if (u.getScheme().startsWith("file")) {
                new FileStreamer().stream(Paths.get(u).toFile(), streamRDF);
            } else {
                try (RDFInputStream ris = loader.fetchResourceCachedOntology(u.toString())) {
                    if (ris != null) {
                        dispatcher.parse(ris);
                        return; //done
                    }
                }
                doStream(source, streamRDF, u.toString(), null); // fetch the URI
            }
        }

        @Override public boolean canStream(@Nonnull Object source) {
            URI u = (URI) source;
            if (u.getScheme().startsWith("file"))
                return new FileStreamer().canStream(Paths.get(u).toFile());
            RDFSyntax syntax = RDFSyntax.guess(u);
            return syntax != null && syntax.hasJenaLang();
        }
    }

    @TargetClass(String.class)
    public static class StringStreamer extends RDFStreamerBase {
        @Override
        public void stream(@Nonnull Object source,
                           @Nonnull StreamRDF streamRDF) throws SourceIterationException {
            String string = source.toString();
            if (URI_RX.matcher(string).find()) {
                try {
                    new URIStreamer().stream(new URI(string), streamRDF);
                    return;
                } catch (URISyntaxException ignored) {}
            }
            new FileStreamer().stream(new File(string), streamRDF);
        }

        @Override public boolean canStream(@Nonnull Object source) {
            if (!(source instanceof String)) return false;
            String string = source.toString();
            if (URI_RX.matcher(string).find()) {
                try {
                    return new URIStreamer().canStream(new URI(string));
                } catch (URISyntaxException ignore) { }
            }
            return new FileStreamer().canStream(new File(string));
        }
    }

    @TargetClass(RDFInputStream.class)
    public static class RDFInputStreamer extends RDFStreamerBase {
        @Override
        public void stream(@Nonnull Object source,
                           @Nonnull StreamRDF streamRDF) throws SourceIterationException {
            RDFInputStream ris = (RDFInputStream) source;
            Lang lang = ris.getSyntaxOrGuess().asJenaLang();
            doStream(source, streamRDF, ris.getInputStream(), ris.getBaseUri(), lang);
        }

        @Override public boolean canStream(@Nonnull Object source) {
            if (!(source instanceof RDFInputStream)) return false;
            return ((RDFInputStream) source).getSyntaxOrGuess().hasJenaLang();
        }
    }

    private static void doStream(@Nonnull Object source, @Nonnull StreamRDF streamRDF,
                                 @Nonnull String uri, @Nullable Lang lang) {
        try {
            RDFDataMgr.parse(streamRDF, uri, lang);
        } catch (InterruptStreamException ignored) {
            streamRDF.finish();
        } catch (RuntimeException e) {
            streamRDF.finish();
            throw new SourceIterationException(source, "Exception streaming "+ source, e);
        }
    }
    private static void doStream(@Nonnull Object source, @Nonnull StreamRDF streamRDF,
                                 @Nonnull InputStream inputStream, @Nullable String baseUri,
                                 @Nullable Lang lang) {
        try {
            RDFDataMgr.parse(streamRDF, inputStream, baseUri, lang);
        } catch (InterruptStreamException ignored) {
            streamRDF.finish();
        } catch (RuntimeException e) {
            streamRDF.finish();
            throw new SourceIterationException(source, "Exception streaming "+ source, e);
        }
    }

    private static @Nonnull RDFSyntax getFileSyntax(@Nonnull File file) {
        if (HDT_RX.matcher(file.getName()).find())
            return HDT;
        Lang lang = RDFLanguages.filenameToLang(file.getName());
        RDFSyntax syntax = RDFSyntax.fromJenaLang(lang);
        if (syntax == null) {
            try (RDFInputStream in = new RDFInputStream(new FileInputStream(file))) {
                syntax = in.getSyntaxOrGuess();
            } catch (IOException ignored) { }
        }
        return syntax == null ? UNKNOWN : syntax;
    }
}
