package ness.cache;

import java.util.Collection;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class Cache implements InternalCacheProvider {
    private final InternalCacheProvider provider;
    private final CacheStatisticsManager cacheStatistics;

    @Inject
    Cache(InternalCacheProvider provider, CacheStatisticsManager cacheStatistics) {
        this.provider = provider;
        this.cacheStatistics = cacheStatistics;
    }
    
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
