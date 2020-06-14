package br.ufsc.lapesd.riefederator.model.term.std;

import javax.annotation.Nonnull;

public class StdPlain extends StdURI {
    public static final @Nonnull String NID = "plain";
    public static final @Nonnull String URI_PREFIX = "urn:"+NID+":";

    public StdPlain(@Nonnull String string) {
        super(URI_PREFIX+string);
    }
}
