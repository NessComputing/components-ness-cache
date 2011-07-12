package ness.cache;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import org.weakref.jmx.Managed;

@ThreadSafe
public class CacheStatistics {

    private final AtomicLong stores, fetches, hits, clears;

    public CacheStatistics(String namespace) {
        stores = new AtomicLong();
        fetches = new AtomicLong();
        hits = new AtomicLong();
        clears = new AtomicLong();
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
    }
}
