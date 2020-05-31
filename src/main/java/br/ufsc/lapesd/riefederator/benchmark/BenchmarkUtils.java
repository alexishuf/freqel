package br.ufsc.lapesd.riefederator.benchmark;

import org.apache.commons.lang3.SystemUtils;

import java.io.IOException;

public class BenchmarkUtils {
    private static final int DEFAULT_GC_WAIT = 100;
    private static final int DEFAULT_GC_MIN_CALLS = 2;
    private static final double DEFAULT_GC_MIN_DECREASE = 0.01;

    public static void flushSystemBuffers() {
        if (SystemUtils.IS_OS_LINUX) {
            try {
                new ProcessBuilder("sync").start().waitFor();
            } catch (InterruptedException | IOException e) {
                /* pass */
            }
        }
    }

    public static long usedMemory() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    @SuppressWarnings("BusyWait")
    public static void preheatCooldown(int waitMs, int minCalls, double minDecrease) {
        long after, before = usedMemory();
        double decrease = 0;
        for (int i = 0; i < minCalls || Math.abs(decrease) > minDecrease; i++) {
            System.gc();
            flushSystemBuffers();
            try {
                Thread.sleep(waitMs);
            } catch (InterruptedException e) {
                /* pass */
            }
            decrease = (before - (after = usedMemory())) / (double) before;
            before = after;
        }
    }
    public static void preheatCooldown() {
        preheatCooldown(DEFAULT_GC_WAIT, DEFAULT_GC_MIN_CALLS, DEFAULT_GC_MIN_DECREASE);
    }
}
