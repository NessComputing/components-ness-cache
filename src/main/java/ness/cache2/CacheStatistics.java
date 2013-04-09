package ness.cache2;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import org.weakref.jmx.Managed;

import com.nesscomputing.logging.Log;

/**
 * Bean to hold cache statistics on a per-namespace basis
 */
@ThreadSafe
public class CacheStatistics {
    private static final Log LOG = Log.findLog();

    private static final long[] HISTOGRAM_MS_BOUNDS = new long[] { 1L, 5L, 10L, 50L, 100L, 250L, 500L, 1000L, 5000L, 10000L, 50000L, Long.MAX_VALUE };
    private static final int MS_ELAPSED_TO_LOG = 1000;
    private static final int HISTOGRAM_COUNT = CacheOperation.values().length;
    private final AtomicIntegerArray[] operationCounts;
    private final AtomicLong stores, fetches, hits, clears, oversizedStores;
    private final String namespace;

    public CacheStatistics(String namespace) {
        this.namespace = namespace;
        stores = new AtomicLong();
        fetches = new AtomicLong();
        hits = new AtomicLong();
        clears = new AtomicLong();
        oversizedStores = new AtomicLong();
        operationCounts = new AtomicIntegerArray[HISTOGRAM_COUNT];
        for (int i=0; i<HISTOGRAM_COUNT; i++) {
            operationCounts[i] = new AtomicIntegerArray(HISTOGRAM_MS_BOUNDS.length);
        }
    }

    public enum CacheOperation {
        STORE("store", 0),
        FETCH("fetch", 1),
        CLEAR("clear", 2);

        private final String description;
        private final int index;

        CacheOperation(String description, int index) {
            this.description = description;
            this.index = index;
        }

        public String getDescription() {
            return description;
        }

        public int getIndex() {
            return index;
        }
    }

    public void recordElapsedTime(long startTime, int keyCount, CacheOperation operation) {
        long elapsed = System.currentTimeMillis() - startTime;
        if (elapsed > MS_ELAPSED_TO_LOG) {
            LOG.warn("Cache operation %s, for %d keys, took %.2f seconds", operation.getDescription(), keyCount, (double)elapsed / 1000.0);
        }
        int index = operation.getIndex();
        int i = 0;
        for (long bound : HISTOGRAM_MS_BOUNDS) {
            if (elapsed <= bound) {
                operationCounts[index].addAndGet(i, keyCount);
                break;
            }
            i++;
        }
    }

    @Managed
    public String getNamespace() {
        return namespace;
    }

    @Managed
    public long getStores() {
        return stores.get();
    }

    public void setStores(long stores) {
        this.stores.set(stores);
    }

    @Managed
    public long getFetches() {
        return fetches.get();
    }

    public void setFetches(long fetches) {
        this.fetches.set(fetches);
    }

    @Managed
    public long getHits() {
        return hits.get();
    }

    @Managed
    public long getOversizedStores() {
        return oversizedStores.get();
    }

    public void setHits(long hits) {
        this.hits.set(hits);
    }

    @Managed
    public long getClears() {
        return clears.get();
    }

    public void setClears(long clears) {
        this.clears.set(clears);
    }

    public void incrementStores(int stores) {
        this.stores.addAndGet(stores);
    }

    public void incrementFetches(int fetches) {
        this.fetches.addAndGet(fetches);
    }

    public void incrementHits(int hits) {
        this.hits.addAndGet(hits);
    }

    public void incrementClears(int clears) {
        this.clears.addAndGet(clears);
    }

    public void incrementOversizedStores(int oversizedStores)
    {
        this.oversizedStores.addAndGet(oversizedStores);
    }

    @Managed
    public double getHitPercentage() {
        return 100.0 * getHits() / getFetches();
    }

    @Managed
    public String getStoresHistogram() {
        return getHistogram(CacheOperation.STORE);
    }

    @Managed
    public String getFetchesHistogram() {
        return getHistogram(CacheOperation.FETCH);
    }

    @Managed
    public String getClearsHistogram() {
        return getHistogram(CacheOperation.CLEAR);
    }

    private String getHistogram(CacheOperation operation) {
        StringBuilder builder = new StringBuilder();
        AtomicIntegerArray array = operationCounts[operation.getIndex()];
        int i = 0;
        long lastBound = 0;
        for (long bound : HISTOGRAM_MS_BOUNDS) {
            int count = array.get(i++);
            if (count > 0) {
                if (builder.length() > 0) {
                    builder.append(", ");
                }
                buildBounds(builder, lastBound, bound);
                builder.append(": ").append(count);
            }
            lastBound = bound;
        }
        if (builder.length() == 0) {
            builder.append("No Samples");
        }
        return builder.toString();
    }

    private void buildBounds(StringBuilder builder, long low, long high) {
        if (low < 1000 && high < 1000) {
            builder.append(low)
                .append("-")
                .append(high)
                .append("ms");
        }
        else if (low < 1000 && high >= 1000) {
            builder.append(low)
                .append("ms")
                .append("-")
                .append(high/1000)
                .append("s");
        }
        else {
            long lowSeconds = low/1000;
            if (high == Long.MAX_VALUE) {
                builder.append(lowSeconds)
                    .append("s")
                    .append("-")
                    .append("max");
            }
            else {
                builder.append(lowSeconds)
                    .append("-")
                    .append(high/1000)
                    .append("s");
            }
        }
    }

    @Managed
    public void clear() {
        stores.set(0);
        fetches.set(0);
        hits.set(0);
        clears.set(0);
        oversizedStores.set(0);
        for (int i=0; i<HISTOGRAM_COUNT; i++) {
            for (int j=0; j<HISTOGRAM_MS_BOUNDS.length; j++) {
                operationCounts[i].set(j, 0);
            }
        }
    }
}
