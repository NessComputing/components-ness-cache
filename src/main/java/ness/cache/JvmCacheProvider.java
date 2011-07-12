package ness.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import com.google.inject.Singleton;

@Singleton
public class JvmCacheProvider implements InternalCacheProvider {
    
    private final ConcurrentMap<String, byte[]> cache = new MapMaker()
           .expireAfterAccess(10, TimeUnit.MINUTES)
           .softValues()
           .makeMap();
    
    @Override
    public void set(String namespace, Map<String, CacheStore> stores) {
        for (Entry<String, CacheStore> e : stores.entrySet()) {
            cache.put(makeKey(namespace, e.getKey()), e.getValue().getBytes());
        }
    }

    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys) {
        ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
        for (String key : keys) {
            byte[] value = cache.get(makeKey(namespace, key));
            if (value != null) {
                builder.put(key, value);
            }
        }
        return builder.build();
    }

    @Override
    public void clear(String namespace, Collection<String> keys) {
        for (String key : keys) {
            cache.remove(makeKey(namespace, key));
        }
    }

    private String makeKey(String namespace, String key) {
        return namespace + "\u0000" + key;
    }
}
