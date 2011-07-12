package ness.cache;

import java.util.Collection;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Provides a provider-neutral caching layer which has asynchronous writes and synchronous
 * reads.  The API does not promise any sorts of consistency, although the provider may
 * be configured (via {@link CacheConfiguration}) to provide various guarantees.
 */
@Singleton
public class Cache implements InternalCacheProvider {
    private final InternalCacheProvider provider;
    private final CacheStatisticsManager cacheStatistics;

    @Inject
    Cache(InternalCacheProvider provider, CacheStatisticsManager cacheStatistics) {
        this.provider = provider;
        this.cacheStatistics = cacheStatistics;
    }
    
    /**
     * Provide a view of this cache which automatically has the given namespace filled in.  Intended to be used
     * in constructors, e.g.
     * <pre>
     * private final NamespacedCache myCache;
     * @Inject
     * MyCoolClass(Cache cache) {
     *     myCache = cache.withNamespace("extraCoolWithCheese");
     * }
     * </pre>
     */
    public NamespacedCache withNamespace(String namespace) {
        return new NamespacedCache(this, namespace);
    }

    @Override
    public void set(String namespace, Map<String, CacheStore> stores) {
        cacheStatistics.getCacheStatistics(namespace).incrementStores(stores.size());
        provider.set(namespace, stores);
    }

    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys) {
        cacheStatistics.getCacheStatistics(namespace).incrementFetches(keys.size());
        Map<String, byte[]> result = provider.get(namespace, keys);
        cacheStatistics.getCacheStatistics(namespace).incrementHits(result.size());
        return result;
    }

    @Override
    public void clear(String namespace, Collection<String> keys) {
        cacheStatistics.getCacheStatistics(namespace).incrementClears(keys.size());
        provider.clear(namespace,keys);
    }
}
