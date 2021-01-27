package br.ufsc.lapesd.freqel.hdt.util;

import com.google.common.base.Stopwatch;
import org.rdfhdt.hdt.listener.ProgressListener;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

public class LoggerHDTProgressListener implements ProgressListener {
    private final @Nonnull Logger logger;
    private int debugWindow = 0, infoWindow = 5;
    private final @Nonnull Stopwatch debugSW = Stopwatch.createStarted();
    private final @Nonnull Stopwatch infoSW = Stopwatch.createStarted();
    private final @Nonnull String logTemplate;

    public LoggerHDTProgressListener(@Nonnull Logger logger, @Nonnull String logPrefix) {
        this.logger = logger;
        this.logTemplate = logPrefix + ": {}% {}";
    }

    @Override public void notifyProgress(float level, String message) {
        if (debugSW.elapsed(TimeUnit.SECONDS) >= debugWindow) {
            logger.debug(logTemplate, level, message);
            debugSW.reset().start();
            debugWindow = 2;
        }
        if (infoSW.elapsed(TimeUnit.SECONDS) >= infoWindow) {
            logger.debug(logTemplate, level, message);
            infoSW.reset().start();
            infoWindow = 20;
        }
    }
}
