package br.ufsc.lapesd.riefederator.util.parse;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.util.FileUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

public enum RDFSyntax {
    TTL,
    NT,
    RDFXML,
    TRIX,
    JSONLD,
    HDT,
    UNKNOWN;

    private static final char[] HDT_COOKIE = {'$', 'H', 'D', 'T'};
    private static final char[] TRIX_COOKIE = {'<', 't', 'r', 'i', 'x'};
    private static final char[] RDF_COOKIE = {'<', 'r', 'd', 'f', };
    private static final RequestConfig HEAD_REQ_CONFIG = RequestConfig.custom()
            .setConnectTimeout(500).setSocketTimeout(500).setConnectionRequestTimeout(500).build();

    public boolean hasJenaLang() {
        return asJenaLang() != null;
    }

    public @Nullable Lang asJenaLang() {
        switch (this) {
            case    TTL: return Lang.TTL;
            case     NT: return Lang.NT;
            case RDFXML: return Lang.RDFXML;
            case JSONLD: return Lang.JSONLD;
            default:
                return null;
        }
    }

    public static @Nullable RDFSyntax fromJenaLang(@Nullable Lang lang) {
        if (lang == null) return null;
        else if (lang.equals(Lang.TTL)) return TTL;
        else if (lang.equals(Lang.NT)) return NT;
        else if (lang.equals(Lang.RDFXML)) return RDFXML;
        else if (lang.equals(Lang.JSONLD)) return JSONLD;
        else return null;
    }

    public @Nullable String getExtension() {
        Lang lang = asJenaLang();
        if (lang != null)
            return lang.getFileExtensions().get(0);
        else if (this == HDT)
            return "hdt";
        else return null;
    }

    public @Nonnull String getSuffix() {
        String extension = getExtension();
        return extension == null ? "" : "."+extension;
    }

    public static @Nullable RDFSyntax guess(@Nonnull InputStream in) {
        if (!in.markSupported())
            return null;
        in.mark(128);
        try {
            return doGuess(in);
        } finally {
            try {
                in.reset();
            } catch (IOException e) {
                throw new RuntimeException("Unexpected IOException on reset()", e);
            }
        }
    }

    @Nullable private static RDFSyntax doGuess(@Nonnull InputStream in) {
        boolean xml = false;
        int hdtPos = 0, trixPos = 0, rdfPos = 0;
        for (int i = 0; i < 127; i++) {
            int c;
            try {
                c = in.read();
            } catch (IOException e) {
                return null;
            }
            if (c == -1) {
                return xml ? UNKNOWN : TTL;
            } else if (xml) {
                c = Character.toLowerCase(c);
                if (c == '!' || c == '?') {
                    trixPos = rdfPos = 0; //definitely XML
                } else if (c == TRIX_COOKIE[trixPos]) {
                    ++trixPos;
                    if (trixPos == TRIX_COOKIE.length)
                        return TRIX;
                } else if (c == RDF_COOKIE[rdfPos]) {
                    ++rdfPos;
                    if (rdfPos == RDF_COOKIE.length)
                        return RDFXML;
                } else { //not XML
                    return NT;
                }
            } else if (c == HDT_COOKIE[hdtPos]) {
                ++hdtPos;
                if (hdtPos == HDT_COOKIE.length)
                    return HDT;
            } else if (!Character.isSpaceChar(c)) {
                if (c == '<') {
                    xml = true; // or NT that starts with <> on subject
                    rdfPos = trixPos = 1;
                } else if (c == '{') {
                    return JSONLD;
                } else if (c == '@' || c == '[') {
                    return TTL;
                } else if (c == '_') {
                    return NT;
                } else {
                    return UNKNOWN;
                }
            }
        }
        return null; //can't guess
    }

    public static @Nullable RDFSyntax guess(@Nonnull String uri) {
        Lang lang = RDFLanguages.filenameToLang(uri);
        if (lang != null)
            return fromJenaLang(lang);
        if (FileUtils.getFilenameExt(uri).equalsIgnoreCase("hdt"))
            return HDT;
        try {
            return guess(new URI(uri));
        } catch (URISyntaxException e) {
            return null;
        }
    }

    public static @Nullable RDFSyntax guess(@Nonnull URI uri) {
        Lang lang = RDFLanguages.filenameToLang(uri.getPath());
        if (lang != null)
            return fromJenaLang(lang);
        if (FileUtils.getFilenameExt(uri.getPath()).equalsIgnoreCase("hdt"))
            return HDT;
        if (uri.getScheme().startsWith("http")) {
            HttpHead headRequest = new HttpHead(uri);
            headRequest.setConfig(HEAD_REQ_CONFIG);
            try (CloseableHttpClient client = HttpClients.createMinimal();
                 CloseableHttpResponse response = client.execute(headRequest)) {
                Header header = response.getLastHeader("Content-Type");
                if (header != null) {
                    for (HeaderElement element : header.getElements()) {
                        lang = RDFLanguages.contentTypeToLang(element.getValue());
                        if (lang != null)
                            return fromJenaLang(lang);
                    }
                }
            } catch (IOException ignored) { }
        }
        return null;
    }
}
