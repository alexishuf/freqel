package br.ufsc.lapesd.riefederator.cassandra;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        System.out.println("EMBEDDED_CASSANDRA_SERVER_HELPER_READY");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
            if (line.equals("DIE")) {
                System.out.println("Received DIE command. Cleaning up");
                EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
                System.out.println("Bye!");
                System.exit(0);
            } else {
                System.out.println("Bad command: "+line);
            }
        }
    }
}
