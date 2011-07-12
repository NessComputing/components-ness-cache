package ness.cache;

import java.util.Collection;
import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class Cache implements InternalCacheProvider {
    private final InternalCacheProvider provider;

    @Inject
    Cache(InternalCacheProvider provider) {
        this.provider = provider;
    }
    
    public NamespacedCache withNamespace(String namespace) {
        return new NamespacedCache(this, namespace);
    }

    @Override
    public void set(String namespace, Map<String, CacheStore> stores) {
        provider.set(namespace, stores);
    }

    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys) {
        return provider.get(namespace, keys);
    }

    @Override
    public void clear(String namespace, Collection<String> keys) {
        provider.clear(namespace,keys);
    }
}
