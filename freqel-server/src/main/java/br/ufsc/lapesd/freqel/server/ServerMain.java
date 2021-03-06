package br.ufsc.lapesd.freqel.server;

import br.ufsc.lapesd.freqel.federation.Federation;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecException;
import br.ufsc.lapesd.freqel.federation.spec.FederationSpecLoader;
import br.ufsc.lapesd.freqel.server.endpoints.Qonfig;
import br.ufsc.lapesd.freqel.server.endpoints.SPARQLEndpoint;
import br.ufsc.lapesd.freqel.server.endpoints.UIFiles;
import org.glassfish.jersey.server.ResourceConfig;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

import static org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory.createHttpServer;

public class ServerMain {
    @Option(name = "--help", aliases = {"-h"}, usage = "Shows help", help = true)
    private boolean help = false;

    @Option(name = "--address", usage = "Server listen address")
    private @Nonnull String listenAddress = "0.0.0.0";

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
            System.exit(1);
        }
        if (app.help) printHelp(System.out, parser);
        else          app.run();
    }

    public @Nonnull ResourceConfig getApplication() throws IOException, FederationSpecException {
        Federation federation = new FederationSpecLoader().load(config);
        return new ResourceConfig()
                .property(Federation.class.getName(), federation)
                .register(SPARQLEndpoint.class)
                .register(Qonfig.class)
                .register(UIFiles.class);
    }

    public void run() throws Exception {
        URI serverURI = new URI("http://"+listenAddress+":"+port);
        ResourceConfig app = getApplication();
        org.glassfish.grizzly.http.server.HttpServer server = createHttpServer(serverURI, app, true);
        server.start();

        System.out.printf("SPARQL endpoint listening on http://%s:%d/sparql/query " +
                          "via GET and POST (form and plain)\n", listenAddress, port);
        System.out.printf("Query interface listening on http://%s:%d/ui/index.html",
                          listenAddress, port);
        Thread.currentThread().join();
    }


}
