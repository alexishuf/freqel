package br.ufsc.lapesd.freqel.webapis.requests;

import java.util.function.Consumer;

@FunctionalInterface
public interface HTTPRequestObserver extends Consumer<HTTPRequestInfo> {
}
