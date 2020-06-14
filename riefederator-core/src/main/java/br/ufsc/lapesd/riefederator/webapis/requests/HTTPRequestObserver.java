package br.ufsc.lapesd.riefederator.webapis.requests;

import java.util.function.Consumer;

@FunctionalInterface
public interface HTTPRequestObserver extends Consumer<HTTPRequestInfo> {
}
