package ness.cache2;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import org.weakref.jmx.Managed;

/**
 * Bean to hold cache statistics on a per-namespace basis
 */
@ThreadSafe
public class CacheStatistics {

    private final AtomicLong stores, fetches, hits, clears, oversizedStores;
    private final String namespace;

    public CacheStatistics(String namespace) {
        this.namespace = namespace;
        stores = new AtomicLong();
        fetches = new AtomicLong();
        hits = new AtomicLong();
        clears = new AtomicLong();
        oversizedStores = new AtomicLong();
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
    public void clear() {
        stores.set(0);
        fetches.set(0);
        hits.set(0);
        clears.set(0);
        oversizedStores.set(0);
    }
}
