package br.ufsc.lapesd.riefederator.server;

import br.ufsc.lapesd.riefederator.federation.Federation;
import br.ufsc.lapesd.riefederator.federation.spec.FederationSpecLoader;
import br.ufsc.lapesd.riefederator.server.endpoints.Qonfig;
import br.ufsc.lapesd.riefederator.server.endpoints.SPARQLEndpoint;
import br.ufsc.lapesd.riefederator.server.endpoints.UIFiles;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.glassfish.jersey.server.ResourceConfig;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.ws.rs.ext.RuntimeDelegate;
import java.io.File;
import java.io.PrintStream;
import java.net.InetSocketAddress;

public class ServerMain {
    private static final Logger logger = LoggerFactory.getLogger(ServerMain.class);

    @Option(name = "--help", aliases = {"-h"}, usage = "Shows help", help = true)
    private boolean help = false;

    @Option(name = "--address", usage = "Server listen address")
    private @Nonnull String listenAddress = "127.0.0.1";

    @Option(name = "--port", aliases = {"-p"}, usage = "Server listen port")
    private int port = 4040;

    @Option(name = "--config", usage = "JSON or YAML with configuration for the federation",
            required = true)
    private File config;


    private static void printHelp(@Nonnull PrintStream out, @Nonnull CmdLineParser parser) {
        out.print("Usage: java -jar $JAR_PATH ");
        parser.printSingleLineUsage(out);
        out.println("Options: ");
        parser.printUsage(out);
    }

    public static void main(String[] args) throws Exception {
        ServerMain app = new ServerMain();
        CmdLineParser parser = new CmdLineParser(app);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getLocalizedMessage());
            printHelp(System.err, parser);
        }
        if (app.help) {
            printHelp(System.out, parser);
        } else {
            app.run();
        }
    }

    public void run() throws Exception {
        Federation federation = new FederationSpecLoader().load(config);

        InetSocketAddress address = new InetSocketAddress(listenAddress, port);
        HttpServer server = HttpServer.create(address, 0);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> server.stop(4)));
        ResourceConfig app = new ResourceConfig()
                .property(Federation.class.getName(), federation)
                .register(SPARQLEndpoint.class)
                .register(Qonfig.class)
                .register(UIFiles.class);
        HttpHandler handler = RuntimeDelegate.getInstance().createEndpoint(app, HttpHandler.class);
        server.createContext("/", handler);

        server.start();
        logger.info("SPARQL endpoint listening on http://{}:{}/sparql/query " +
                    "via GET and POST (form and plain)", listenAddress, port);
        logger.info("Query interface listening on http://{}:{}/ui/index.html", listenAddress, port);
        Thread.currentThread().join();
    }


}
