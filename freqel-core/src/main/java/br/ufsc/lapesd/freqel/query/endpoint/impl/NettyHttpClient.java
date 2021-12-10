package br.ufsc.lapesd.freqel.query.endpoint.impl;

import br.ufsc.lapesd.freqel.util.ref.IdentityHashSet;
import com.google.common.base.Stopwatch;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringSocketChannel;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class NettyHttpClient {
    private static final Logger logger = LoggerFactory.getLogger(NettyHttpClient.class);
    private final @Nonnull String handlerName;
    private final @Nonnull Supplier<? extends SimpleChannelInboundHandler<HttpObject>>
            handlerSupplier;
    private EventLoopGroup group;
    private Transport transport;
    private int references;
    private SslContext sslContext;

    public enum Transport {
        NIO {
            @Override public boolean isAvailable() { return true; }
            @Override public @Nonnull EventLoopGroup createGroup() {
                return new NioEventLoopGroup();
            }
            @Override public @Nonnull Class<? extends SocketChannel> channelClass() {
                return NioSocketChannel.class;
            }
        },
        IO_URING {
            @Override public boolean isAvailable() {
                try {
                    Class.forName("io.netty.incubator.channel.uring.IOUring");
                    return IOUring.isAvailable();
                } catch (ClassNotFoundException e) { return false; }
            }
            @Override public @Nonnull EventLoopGroup createGroup() {
                return new IOUringEventLoopGroup();
            }
            @Override public @Nonnull Class<? extends SocketChannel> channelClass() {
                return IOUringSocketChannel.class;
            }
        },
        KQUEUE {
            @Override public boolean isAvailable() {
                try {
                    Class.forName("io.netty.channel.kqueue.KQueue");
                    return KQueue.isAvailable();
                } catch (ClassNotFoundException e) {return false;}
            }
            @Override public @Nonnull EventLoopGroup createGroup() {
                return new KQueueEventLoopGroup();
            }
            @Override public @Nonnull Class<? extends SocketChannel> channelClass() {
                return KQueueSocketChannel.class;
            }
        },
        EPOLL {
            @Override public boolean isAvailable() {
                try {
                    Class.forName("io.netty.channel.epoll.Epoll");
                    return Epoll.isAvailable();
                } catch (ClassNotFoundException e) {return false;}
            }
            @Override public @Nonnull EventLoopGroup createGroup() {
                return new EpollEventLoopGroup();
            }
            @Override public @Nonnull Class<? extends SocketChannel> channelClass() {
                return EpollSocketChannel.class;
            }
        };

        abstract public boolean isAvailable();
        abstract public @Nonnull EventLoopGroup createGroup();
        abstract public @Nonnull Class<? extends SocketChannel>  channelClass();
    }

    public class Targeted implements AutoCloseable {
        private final @Nonnull ParsedURI parsedURI;
        private final @Nonnull SimpleChannelPool pool;
        private final @Nullable SslContext sslContext;
        private boolean closed = false;
        private long closeTimeout = 0;
        private @Nonnull TimeUnit closeTimeoutTimeUnit = MILLISECONDS;
        private final @Nonnull IdentityHashSet<Channel> channels = new IdentityHashSet<>();

        public Targeted(@Nonnull String uri,
                        @Nonnull String handlerName,
                        @Nonnull Supplier<? extends SimpleChannelInboundHandler<HttpObject>>
                                handlerSupplier,
                        @Nullable SslContext sslContext) {
            this.sslContext = sslContext;
            parsedURI = new ParsedURI(uri, this.sslContext != null);
            Bootstrap bootstrap = new Bootstrap().group(group).channel(transport.channelClass())
                    .remoteAddress(parsedURI.host, parsedURI.port);
            this.pool = new SimpleChannelPool(bootstrap, new AbstractChannelPoolHandler() {
                @Override public void channelCreated(Channel ch) {
                    synchronized (Targeted.this) {
                        if (closed) {
                            logger.debug("Closing channel {} connected after {}.close()",
                                         ch, Targeted.this);
                            ch.close();
                            return;
                        }
                        channels.add(ch);
                    }
                    ChannelPipeline p = ch.pipeline();
                    if (sslContext != null && parsedURI.https)
                        p.addLast(sslContext.newHandler(ch.alloc()));
                    p.addLast("http", new HttpClientCodec());
                    p.addLast("decompressor", new HttpContentDecompressor());
                    p.addLast(handlerName, handlerSupplier.get());
                    ch.closeFuture().addListener(f -> {
                        synchronized (Targeted.this) {
                            channels.remove(ch);
                        }
                    });
                }
            });
        }

        public @Nonnull String baseURI() {
            return parsedURI.uri;
        }

        public @Nonnull String basePath() {
            return parsedURI.rawPath;
        }

        public @Nonnull ChannelPool getPool() {
            return pool;
        }

        public void release(@Nonnull Channel channel) {
            pool.release(channel);
        }

        public @Nonnull Future<Channel> connect() {
            if (closed)
                throw new IllegalStateException(this+" is closed");
            return pool.acquire();
        }

        public @Nonnull Future<Channel>
        request(@Nonnull HttpMethod method, @Nonnull String uriOrPath,
                @Nullable Function<ByteBufAllocator, ByteBuf> bodyGenerator,
                @Nullable BiConsumer<SocketChannel, HttpRequest> setup) {
            String host, path;
            if (uriOrPath.startsWith("http")) {
                ParsedURI p = new ParsedURI(uriOrPath, sslContext != null);
                if (!p.host.equals(parsedURI.host))
                    logger.warn("Mismatching host in URI {} on {}.request()", uriOrPath, this);
                if (p.https != parsedURI.https)
                    logger.warn("Ignoring scheme of URI {} on {}.request()", uriOrPath, this);
                if (p.port != parsedURI.port)
                    logger.warn("Ignoring port of URI {} on {}.request()", uriOrPath, this);
                host = p.host;
                path = p.rawPath;
            } else {
                host = parsedURI.host;
                path = uriOrPath;
            }

            return connect().addListener(f -> {
                SocketChannel ch = (SocketChannel)f.getNow();
                if (ch == null) {
                    logger.error("Failed to open connection, will not send request");
                    return;
                }
                HttpRequest req;
                if (bodyGenerator != null) {
                    ByteBuf bb = bodyGenerator.apply(ch.alloc());
                    req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, bb);
                    req.headers().set(HttpHeaderNames.CONTENT_LENGTH, bb.readableBytes());
                } else {
                    req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path);
                }
                HttpHeaders headers = req.headers();
                headers.set(HttpHeaderNames.HOST, host);
                headers.set(HttpHeaderNames.CONNECTION, "keep-alive");
                if (setup != null)
                    setup.accept(ch, req);
                ch.writeAndFlush(req);
            });
        }

        public @Nonnull Targeted setCloseTimeout(long timeout, @Nonnull TimeUnit timeUnit) {
            this.closeTimeout = timeout;
            this.closeTimeoutTimeUnit = timeUnit;
            return this;
        }

        @Override public void close() {
            if (closed)
                return;
            closed = true;
            long closeTimeoutMs = MILLISECONDS.convert(closeTimeout, closeTimeoutTimeUnit);
            Stopwatch sw = Stopwatch.createStarted();
            List<ChannelFuture> closeFutures = new ArrayList<>();
            ArrayList<Channel> copy;
            synchronized (this) {
                copy = new ArrayList<>(this.channels);
            }
            for (Channel channel : copy) {
                closeFutures.add(channel.close());
            }
            Future<Void> poolClose = pool.closeAsync();
            if (!poolClose.awaitUninterruptibly(closeTimeout, closeTimeoutTimeUnit)) {
                logger.debug("Stopped waiting {}.pool close after {} {}",
                              this, closeTimeout, closeTimeoutTimeUnit);
            }
            int alive = 0;
            for (ChannelFuture future : closeFutures) {
                long ms = closeTimeoutMs - sw.elapsed(MILLISECONDS);
                if (!future.awaitUninterruptibly(ms, MILLISECONDS))
                    alive++;
            }
            if (alive > 0)
                logger.debug("{}.close(): {} channels still alive after close(), not waited due to " +
                             "close() timeout of {} {}",
                             this, alive, closeTimeout, closeTimeoutTimeUnit);
            NettyHttpClient.this.release(closeTimeoutMs - sw.elapsed(MILLISECONDS));
        }

        @Override public @Nonnull String toString() {
            return "Netty.HttpClient.Targeted{"+parsedURI.uri+"}";
        }
    }

    public NettyHttpClient(@Nonnull Supplier<? extends SimpleChannelInboundHandler<HttpObject>>
                               supplier) {
        this(supplier, "chunkHandler");
    }

    public NettyHttpClient(@Nonnull Supplier<? extends SimpleChannelInboundHandler<HttpObject>> 
                                   supplier,
                           @Nonnull String handlerName) {
        this.handlerName = handlerName;
        this.handlerSupplier = supplier;
    }

    public synchronized @Nonnull Targeted acquire(@Nonnull String uri) {
        if (references++ == 0) {
            assert group == null;
            transport = chooseTransport();
            sslContext = getSslContext();
            group = transport.createGroup();
        }
        return new Targeted(uri, handlerName, handlerSupplier, sslContext);
    }

    private @Nonnull Transport chooseTransport() {
        Transport selected = Transport.NIO;
        for (Transport transport : Transport.values()) {
            if (transport.isAvailable()) {
                selected = transport;
                break;
            }
        }
        logger.debug("Using "+selected+" for transport");
        return selected;
    }

    private @Nullable SslContext getSslContext() {
        try {
            return SslContextBuilder.forClient().build();
        } catch (SSLException e) {
            logger.error("Failed to initialize SSL context for Netty, will not support " +
                         "HTTPS requests.", e);
            return null;
        }
    }

    private synchronized void release(long closeTimeoutMs) {
        if (--references == 0) {
            assert group != null;
            if (!group.shutdownGracefully().awaitUninterruptibly(closeTimeoutMs, MILLISECONDS)) {
                logger.debug("{}.group.shutdownGracefully(): not completed under {} ms",
                             this, closeTimeoutMs);
            }
            group = null;
            sslContext = null;
        }
    }

    private static final class ParsedURI {
        final boolean https;
        final @Nonnull String uri, host, rawPath;
        final int port;

        public ParsedURI(@Nonnull String uri, boolean allowHTTPS) {
            URI parsed = null;
            try {
                parsed = new URI(uri);
            } catch (URISyntaxException e) {
                Matcher m = Pattern.compile("[^:/]/").matcher(uri);
                if (m.find()) {
                    try {
                        parsed = new URI(uri.substring(0, m.start()+1));
                    } catch (URISyntaxException ignored) { }
                }
                if (parsed == null)
                    throw new IllegalArgumentException("Invalid URI "+uri+": "+e.getMessage());
            }
            String scheme = parsed.getScheme();
            if (!scheme.startsWith("http"))
                throw new IllegalArgumentException("Scheme "+ scheme +" not supported");
            if (scheme.startsWith("https")) {
                if (allowHTTPS)
                    https = true;
                else
                    throw new IllegalArgumentException("HTTPS not supported");
            } else {
                https = false;
            }
            this.uri = uri;
            port = parsed.getPort() > 0 ? parsed.getPort() : scheme.startsWith("https") ? 443 : 80;
            host = parsed.getHost();
            rawPath = parsed.getRawPath();
        }
    }

}
