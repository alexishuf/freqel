package br.ufsc.lapesd.freqel.webapis.requests;

import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * POJO for request data
 */
public class HTTPRequestInfo {
    private @Nonnull String method, uri;
    private @Nonnull Map<String, String> headers = Collections.emptyMap();
    private @Nullable Date requestDate;
    private int status = 0, responseBytes = 0;
    private @Nonnull String contentType = "";
    private double createUriMs = -1, requestMs = -1, parseMs = -1;
    private int jsonRootArrayMembers = -1;
    private int jsonRootObjectMembers = -1;
    private int parsedTriples = -1;
    private Exception exception = null;

    public HTTPRequestInfo(@Nonnull String method, @Nonnull String uri) {
        this.method = method;
        this.uri = uri;
    }

    @CanIgnoreReturnValue
    public
    @Nonnull HTTPRequestInfo setMethod(@Nonnull String method) {
        this.method = method;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setUri(@Nonnull String uri) {
        this.uri = uri;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setHeaders(@Nonnull Map<String, String> headers) {
        this.headers = headers;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setRequestDate(@Nonnull Date requestDate) {
        this.requestDate = requestDate;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setStatus(int status) {
        this.status = status;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setResponseBytes(int responseBytes) {
        this.responseBytes = responseBytes;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setContentType(@Nonnull String contentType) {
        this.contentType = contentType;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setCreateUriMs(double createUriMs) {
        this.createUriMs = createUriMs;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setRequestMs(double requestMs) {
        this.requestMs = requestMs;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setParseMs(double parseMs) {
        this.parseMs = parseMs;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setCreateUriMs(@Nonnull Stopwatch sw) {
        return setCreateUriMs(sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setRequestMs(@Nonnull Stopwatch sw) {
        return setRequestMs(sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setParseMs(@Nonnull Stopwatch sw) {
        return setParseMs(sw.elapsed(TimeUnit.MICROSECONDS)/1000.0);
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setJsonRootArrayMembers(int jsonRootArrayMembers) {
        this.jsonRootArrayMembers = jsonRootArrayMembers;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setJsonRootObjectMembers(int jsonRootObjectMembers) {
        this.jsonRootObjectMembers = jsonRootObjectMembers;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setParsedTriples(int parsedTriples) {
        this.parsedTriples = parsedTriples;
        return this;
    }

    @CanIgnoreReturnValue
    public @Nonnull HTTPRequestInfo setException(@Nullable Exception exception) {
        this.exception = exception;
        return this;
    }

    public @Nonnull String getMethod() {
        return method;
    }

    public @Nonnull String getUri() {
        return uri;
    }

    public @Nonnull Map<String, String> getHeaders() {
        return headers;
    }

    public @Nullable Date getRequestDate() {
        return requestDate;
    }
    @Contract("!null -> !null")
    public String getRequestDateAsISO(String fallback) {
        if (requestDate == null)
            return fallback;
        SimpleDateFormat iso = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
        return iso.format(requestDate);
    }
    public @Nullable String getRequestDateAsISO() {
        return getRequestDateAsISO(null);
    }

    public int getStatus() {
        return status;
    }

    public int getResponseBytes() {
        return responseBytes;
    }

    public @Nonnull String getContentType() {
        return contentType;
    }

    public double getCreateUriMs() {
        return createUriMs;
    }

    public double getRequestMs() {
        return requestMs;
    }

    public double getParseMs() {
        return parseMs;
    }

    public int getJsonRootArrayMembers() {
        return jsonRootArrayMembers;
    }

    public int getJsonRootObjectMembers() {
        return jsonRootObjectMembers;
    }

    public int getParsedTriples() {
        return parsedTriples;
    }

    public @Nullable Exception getException() {
        return exception;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HTTPRequestInfo)) return false;
        HTTPRequestInfo that = (HTTPRequestInfo) o;
        return getStatus() == that.getStatus() &&
                getResponseBytes() == that.getResponseBytes() &&
                Double.compare(that.getCreateUriMs(), getCreateUriMs()) == 0 &&
                Double.compare(that.getRequestMs(), getRequestMs()) == 0 &&
                Double.compare(that.getParseMs(), getParseMs()) == 0 &&
                getJsonRootArrayMembers() == that.getJsonRootArrayMembers() &&
                getJsonRootObjectMembers() == that.getJsonRootObjectMembers() &&
                getParsedTriples() == that.getParsedTriples() &&
                getMethod().equals(that.getMethod()) &&
                getUri().equals(that.getUri()) &&
                getHeaders().equals(that.getHeaders()) &&
                Objects.equals(getRequestDate(), that.getRequestDate()) &&
                getContentType().equals(that.getContentType()) &&
                Objects.equals(getException(), that.getException());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMethod(), getUri(), getHeaders(), getRequestDate(),
                getStatus(), getResponseBytes(), getContentType(), getCreateUriMs(),
                getRequestMs(), getParseMs(), getJsonRootArrayMembers(),
                getJsonRootObjectMembers(), getParsedTriples(), getException());
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(String.format("%s %s [%d %d bytes %s]",
                               method, uri, status, responseBytes, contentType));
        if (createUriMs >= 0)
            b.append(String.format(" createUriMs=%.3f", createUriMs));
        if (requestMs >= 0)
            b.append(String.format(" requestMs=%.3f", requestMs));
        if (requestDate != null)
            b.append(" at ").append(getRequestDateAsISO());
        if (parseMs >= 0)
            b.append(String.format(" parseMs=%.3f", parseMs));
        if (jsonRootArrayMembers >= 0)
            b.append(" jsonRootArrayMembers=").append(jsonRootArrayMembers);
        if (jsonRootObjectMembers >= 0)
            b.append(" jsonRootObjectMembers=").append(jsonRootObjectMembers);
        if (parsedTriples >= 0)
            b.append(" parsedTriples=").append(parsedTriples);
        if (exception != null)
            b.append(" exception=").append(exception.getMessage());
        return b.toString();
    }
}
