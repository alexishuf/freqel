package br.ufsc.lapesd.freqel.query.endpoint.impl;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.testng.Assert.*;

public class NettyHttpClientTest {
    private EventLoopGroup serverAcceptGroup;
    private EventLoopGroup serverWorkerGroup;
    private Channel serverChannel;
    private int port;

    private static class EchoHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {

            HttpHeaders headers = req.headers();
            String type = headers.get(HttpHeaderNames.CONTENT_TYPE, "text/plain");
            String connection = headers.get(HttpHeaderNames.CONNECTION, "keep-alive");
            if (req.decoderResult().isFailure()) {
                ByteBuf body = ctx.alloc().buffer();
                body.writeCharSequence("Bad request: "+req.decoderResult(), UTF_8);
                HttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST, body);
                res.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
                ctx.writeAndFlush(res);
            } else {
                HttpResponse res = new DefaultHttpResponse(HTTP_1_1, OK);
                res.headers().set(HttpHeaderNames.CONNECTION, connection);
                res.headers().set(HttpHeaderNames.CONTENT_TYPE, type);
                res.headers().set(HttpHeaderNames.TRANSFER_ENCODING, "chunked");
                ByteBuf content = req.content().retain();
                ctx.writeAndFlush(res);
                ctx.executor().schedule(() -> {
                    ctx.writeAndFlush(new DefaultHttpContent(content.retain()));
                    ctx.executor().schedule(() -> {
                        ctx.writeAndFlush(new DefaultLastHttpContent(content));
                        if (!connection.equalsIgnoreCase("keep-alive"))
                            ctx.close();
                    }, 1, TimeUnit.MILLISECONDS);
                }, 1, TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            super.exceptionCaught(ctx, cause);
        }
    }

    @BeforeClass(groups = {"fast"})
    public void beforeClass() {
        serverAcceptGroup = new NioEventLoopGroup(1);
        serverWorkerGroup = new NioEventLoopGroup();
        serverChannel = new ServerBootstrap().group(serverAcceptGroup, serverWorkerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new HttpServerCodec())
                                .addLast(new HttpObjectAggregator(65536))
                                .addLast(new EchoHandler());
                    }
                }).bind(0).syncUninterruptibly().channel();
        port = ((InetSocketAddress)serverChannel.localAddress()).getPort();
    }

    @AfterClass(groups = {"fast"})
    public void afterClass() {
        serverChannel.close().syncUninterruptibly();
        serverAcceptGroup.shutdownGracefully();
        serverWorkerGroup.shutdownGracefully();
    }


    private static class EchoClientHandler extends SimpleChannelInboundHandler<HttpObject> {
        private List<Channel> handledChannels;
        private List<String> chunks;
        private List<String> cts;
        private Semaphore handled;
        private List<Throwable> exceptions;

        public void setup(@Nonnull List<Channel> handledChannels, @Nonnull List<String> chunks,
                          @Nonnull List<String> cts, @Nonnull Semaphore handled,
                          @Nonnull List<Throwable> exceptions) {
            this.handledChannels = handledChannels;
            this.chunks = chunks;
            this.cts = cts;
            this.handled = handled;
            this.exceptions = exceptions;
        }

        public void recycle() {
            this.handledChannels = null;
            this.chunks = null;
            this.cts = null;
            this.handled = null;
            this.exceptions = null;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (this.exceptions == null)
                cause.printStackTrace();
            else
                this.exceptions.add(cause);
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            recycle();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
            assert handledChannels != null;
            handledChannels.add(ctx.channel());
            if (msg instanceof HttpResponse) {
                cts.add(((HttpResponse) msg).headers().get(HttpHeaderNames.CONTENT_TYPE));
            }
            if (msg instanceof HttpContent) {
                chunks.add(((HttpContent)msg).content().toString(UTF_8));
                if (msg instanceof LastHttpContent) {
                    handled.release();

                }
            }
        }
    }

    private void doTestEcho(int nThreads) throws InterruptedException {
        NettyHttpClient client = new NettyHttpClient(EchoClientHandler::new, "echo");
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Future<?>> requestFutures = new ArrayList<>();
        try (NettyHttpClient.Targeted targeted = client.acquire("http://localhost:" + port)) {
            for (int i = 0; i < nThreads; i++) {
                requestFutures.add(executor.submit(() -> {
                    List<Channel> handledChannels = new ArrayList<>();
                    List<String> chunks = new ArrayList<>();
                    List<String> cts = new ArrayList<>();
                    List<Throwable> handlerExceptions = new ArrayList<>();
                    Semaphore handled = new Semaphore(0);

                    Channel channel = targeted.request(HttpMethod.POST, "/echo?x=2%203", a -> {
                        ByteBuf bb = a.buffer();
                        bb.writeCharSequence("important message", UTF_8);
                        return bb;
                    }, (ch, req) -> {
                        req.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
                        EchoClientHandler handler = (EchoClientHandler) ch.pipeline().get("echo");
                        handler.setup(handledChannels, chunks, cts, handled, handlerExceptions);
                    }).syncUninterruptibly().getNow();

                    handled.acquireUninterruptibly(1);
                    assertEquals(handledChannels, asList(channel, channel, channel, channel));
                    assertEquals(cts, singletonList("text/plain"));
                    assertEquals(chunks, asList("important message", "important message", ""));
                    assertEquals(handlerExceptions, Collections.emptyList());
                    Thread.sleep(200);
                    assertTrue(channel.isRegistered());
                    assertTrue(channel.isOpen());
                    assertTrue(channel.isWritable());
                    channel.close().syncUninterruptibly();
                    assertFalse(channel.isWritable());
                    assertFalse(channel.isOpen());
                    return null;
                }));
            }
            List<Throwable> causes = new ArrayList<>();
            for (Future<?> f : requestFutures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    causes.add(e.getCause());
                } catch (InterruptedException e) {
                    causes.add(e);
                }
            }
            if (!causes.isEmpty())
                fail(causes.size()+"/"+nThreads+" client threads failed", causes.get(0));
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(4, TimeUnit.SECONDS));
        }
    }

    @Test(timeOut = 5000, groups = {"fast"})
    private void testSingleClientOnce() throws InterruptedException {
        doTestEcho(1);
    }

    @Test(timeOut = 5000, invocationCount = 20)
    private void testSingleClientSequential() throws InterruptedException {
        doTestEcho(1);
    }

    @Test(timeOut = 5000, invocationCount = 100, threadPoolSize = 10)
    private void testSingleClientInParallel() throws InterruptedException {
        doTestEcho(1);
    }

    @Test(timeOut = 5000, invocationCount = 4)
    private void testThousandClients() throws InterruptedException {
        doTestEcho(1000);
    }
}