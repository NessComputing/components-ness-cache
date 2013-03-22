package ness.cache2;

import java.util.Collection;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import com.nesscomputing.logging.Log;

import ness.cache2.CacheStatistics.CacheOperation;

/**
 * Provides a provider-neutral caching layer which has asynchronous writes and synchronous
 * reads.  The API does not promise any sorts of consistency, although the provider may
 * be configured (via {@link CacheConfiguration}) to provide various guarantees.
 */
@SuppressWarnings("deprecation")
@Singleton
public class NessCacheImpl implements NessCache, Cache {
    private static final Log LOG = Log.findLog();
    @VisibleForTesting
    final InternalCacheProvider provider;

    private CacheStatisticsManager cacheStatistics = null;

    @Inject
    protected NessCacheImpl(InternalCacheProvider provider) {
        this.provider = provider;
    }

    @Inject(optional=true)
    void injectCacheStatisticsManager(final CacheStatisticsManager cacheStatistics)
    {
        this.cacheStatistics = cacheStatistics;
    }

    /**
     * Provide a view of this cache which automatically has the given namespace filled in.  Intended to be used
     * in constructors, e.g.
     * <pre>
     * private final NamespacedCache myCache;
     *
     * MyCoolClass(Cache cache) {
     *     myCache = cache.withNamespace("extraCoolWithCheese");
     * }
     * </pre>
     */
    @Override
    public NamespacedCache withNamespace(String namespace) {
        return new NamespacedCache(this, namespace);
    }

    @Override
    public void set(String namespace, Collection<CacheStore<byte []>> stores) {
        long startTime = System.currentTimeMillis();
        CacheStatistics stats = null;
        LOG.trace("set(%s, %s)", namespace, stores);
        if (cacheStatistics != null) {
            stats = cacheStatistics.getCacheStatistics(namespace);
            stats.incrementStores(stores.size());
        }
        provider.set(namespace, stores, stats);
        recordElapsedTime(stats, startTime, stores.size(), CacheOperation.STORE);
    }

    @Override
    public Map<String, Boolean> add(String namespace, Collection<CacheStore<byte []>> stores) {
        long startTime = System.currentTimeMillis();
        CacheStatistics stats = null;
        LOG.trace("add(%s, %s)", namespace, stores);
        if (cacheStatistics != null) {
            stats = cacheStatistics.getCacheStatistics(namespace);
            stats.incrementStores(stores.size());
        }
        Map<String, Boolean> result = provider.add(namespace, stores, stats);
        recordElapsedTime(stats, startTime, stores.size(), CacheOperation.STORE);
        return result;
    }

    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys) {
        long startTime = System.currentTimeMillis();
        CacheStatistics stats = null;
        if (cacheStatistics != null) {
            stats = cacheStatistics.getCacheStatistics(namespace);
            stats.incrementFetches(keys.size());
        }
        Map<String, byte[]> result = provider.get(namespace, keys, stats);
        if (stats != null) {
            stats.incrementHits(result.size());
        }
        recordElapsedTime(stats, startTime, keys.size(), CacheOperation.FETCH);
        LOG.trace("get(%s, %s) hit %d", namespace, keys, result.size());
        return result;
    }

    @Override
    public void clear(String namespace, Collection<String> keys) {
        long startTime = System.currentTimeMillis();
        CacheStatistics stats = null;
        LOG.trace("clear(%s, %s)", namespace, keys);
        if (cacheStatistics != null) {
            stats = cacheStatistics.getCacheStatistics(namespace);
            stats.incrementClears(keys.size());
        }
        provider.clear(namespace, keys, stats);
        recordElapsedTime(stats, startTime, keys.size(), CacheOperation.CLEAR);
    }

    private void recordElapsedTime(CacheStatistics stats, long startTime, int keyCount, CacheOperation operation) {
        if (stats != null) {
            stats.recordElapsedTime(startTime, keyCount, operation);
        }
    }
}
