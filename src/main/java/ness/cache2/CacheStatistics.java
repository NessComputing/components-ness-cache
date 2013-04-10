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
    private final AtomicLong storeKeys, storeOperations, fetchKeys, fetchOperations, hitKeys, hitOperations, clearKeys, clearOperations, oversizedStores;
    private final String namespace;

    public CacheStatistics(String namespace) {
        this.namespace = namespace;
        storeKeys = new AtomicLong();
        storeOperations = new AtomicLong();
        fetchKeys = new AtomicLong();
        fetchOperations = new AtomicLong();
        hitKeys = new AtomicLong();
        hitOperations = new AtomicLong();
        clearKeys = new AtomicLong();
        clearOperations = new AtomicLong();
        oversizedStores = new AtomicLong();
        operationCounts = new AtomicIntegerArray[HISTOGRAM_COUNT];
        for (int i=0; i<HISTOGRAM_COUNT; i++) {
            operationCounts[i] = new AtomicIntegerArray(HISTOGRAM_MS_BOUNDS.length);
        }
    }

    public enum CacheOperation {
        STORE_KEYS("store keys", 0),
        STORE_OPERATIONS("store operations", 1),
        FETCH_KEYS("fetch keys", 2),
        FETCH_OPERATIONS("fetch operations", 3),
        CLEAR_KEYS("clear keys", 4),
        CLEAR_OPERATIONS("clear operations", 5);

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

    public void recordElapsedTime(long elapsed, int itemCount, CacheOperation keysOperation, CacheOperation callsOperation) {
        if (elapsed > MS_ELAPSED_TO_LOG) {
            LOG.warn("Cache operation %s, for %d items, took %.2f seconds", keysOperation.getDescription(), itemCount, (double)elapsed / 1000.0);
        }
        recordInHistogram(elapsed, keysOperation, itemCount);
        recordInHistogram(elapsed, callsOperation, 1);
    }

    private void recordInHistogram(long elapsed, CacheOperation operation, int itemCount) {
        int index = operation.getIndex();
        int i = 0;
        for (long bound : HISTOGRAM_MS_BOUNDS) {
            if (elapsed <= bound) {
                operationCounts[index].addAndGet(i, itemCount);
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
    public long getStoreKeys() {
        return storeKeys.get();
    }

    @Managed
    public long getStoreOperations() {
        return storeOperations.get();
    }

    public void setStores(long stores) {
        this.storeKeys.set(stores);
    }

    @Managed
    public long getFetchKeys() {
        return fetchKeys.get();
    }

    @Managed
    public long getFetchOperations() {
        return fetchOperations.get();
    }

    public void setFetches(long fetches) {
        this.fetchKeys.set(fetches);
    }

    @Managed
    public long getHitKeys() {
        return hitKeys.get();
    }

    @Managed
    public long getHitOperations() {
        return hitOperations.get();
    }

    @Managed
    public long getOversizedStores() {
        return oversizedStores.get();
    }

    public void setHits(long hits) {
        this.hitKeys.set(hits);
    }

    @Managed
    public long getClearKeys() {
        return clearKeys.get();
    }

    public void setClears(long clears) {
        this.clearKeys.set(clears);
    }

    public void incrementStores(int stores) {
        this.storeKeys.addAndGet(stores);
        this.storeOperations.incrementAndGet();
    }

    public void incrementFetches(int fetches) {
        this.fetchKeys.addAndGet(fetches);
        this.fetchOperations.incrementAndGet();
    }

    public void incrementHits(int hits) {
        this.hitKeys.addAndGet(hits);
        this.hitOperations.incrementAndGet();
    }

    public void incrementClears(int clears) {
        this.clearKeys.addAndGet(clears);
        this.clearOperations.incrementAndGet();
    }

    public void incrementOversizedStores(int oversizedStores)
    {
        this.oversizedStores.addAndGet(oversizedStores);
    }

    @Managed
    public double getHitKeysPercentage() {
        return 100.0 * getHitKeys() / getFetchKeys();
    }

    @Managed
    public double getHitOperationsPercentage() {
        return 100.0 * getHitOperations() / getFetchOperations();
    }

    @Managed
    public String getStoreKeysHistogram() {
        return getHistogram(CacheOperation.STORE_KEYS);
    }

    @Managed
    public String getStoreOperationsHistogram() {
        return getHistogram(CacheOperation.STORE_OPERATIONS);
    }

    @Managed
    public String getFetchKeysHistogram() {
        return getHistogram(CacheOperation.FETCH_KEYS);
    }

    @Managed
    public String getFetchOperationsHistogram() {
        return getHistogram(CacheOperation.FETCH_OPERATIONS);
    }

    @Managed
    public String getClearKeysHistogram() {
        return getHistogram(CacheOperation.CLEAR_KEYS);
    }

    @Managed
    public String getClearOperationsHistogram() {
        return getHistogram(CacheOperation.CLEAR_OPERATIONS);
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
        storeKeys.set(0);
        storeOperations.set(0);
        fetchKeys.set(0);
        fetchOperations.set(0);
        hitKeys.set(0);
        hitOperations.set(0);
        clearKeys.set(0);
        clearOperations.set(0);
        oversizedStores.set(0);
        for (int i=0; i<HISTOGRAM_COUNT; i++) {
            for (int j=0; j<HISTOGRAM_MS_BOUNDS.length; j++) {
                operationCounts[i].set(j, 0);
            }
        }
    }
}
