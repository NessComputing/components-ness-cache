package ness.cache;

import java.util.Collection;
import java.util.Map;

interface InternalCacheProvider {
    void set(String namespace, Map<String, CacheStore> stores);
    Map<String, byte[]> get(String namespace, Collection<String> keys);
    void clear(String namespace, Collection<String> keys);
}
