package br.ufsc.lapesd.riefederator.util.parse;

import java.util.Objects;

import static java.lang.String.format;

public class TriplesEstimate {
    private long estimate;
    private int ignoredSources, totalSources;

    public TriplesEstimate(long estimate, int ignoredSources, int totalSources) {
        this.estimate = estimate;
        this.ignoredSources = ignoredSources;
        this.totalSources = totalSources;
    }

    public long getEstimate() {
        return estimate;
    }

    public int getIgnoredSources() {
        return ignoredSources;
    }

    public int getTotalSources() {
        return totalSources;
    }

    public boolean hasEstimate() {
        return estimate > -1;
    }

    public long averagingIgnored() {
        if (estimate < 0) return estimate;
        double avg = estimate / (double) (totalSources - ignoredSources);
        return (long)(totalSources * avg);
    }

    @Override public String toString() {
        return format("TriplesEstimate(%d, ignored=%d/%d)", estimate, ignoredSources, totalSources);
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TriplesEstimate)) return false;
        TriplesEstimate that = (TriplesEstimate) o;
        return getEstimate() == that.getEstimate() && getIgnoredSources() == that.getIgnoredSources() && getTotalSources() == that.getTotalSources();
    }

    @Override public int hashCode() {
        return Objects.hash(getEstimate(), getIgnoredSources(), getTotalSources());
    }
}
