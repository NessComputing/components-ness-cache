package ness.cache2;

import io.trumpet.lifecycle.Lifecycle;
import io.trumpet.lifecycle.LifecycleListener;
import io.trumpet.lifecycle.LifecycleStage;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * An in-JVM cache, currently backed by EHCache
 */
@Singleton
public class JvmCacheProvider implements InternalCacheProvider {

    private final Cache ehCache;

    static {
        System.setProperty("net.sf.ehcache.skipUpdateCheck", "true");
    }

    @Inject
    JvmCacheProvider(Lifecycle lifecycle) {
        ehCache = new Cache(new CacheConfiguration("ness.cache." + hashCode(), 100000)
                .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LFU)
                .overflowToDisk(false)
                .diskPersistent(false));

        final CacheManager cacheManager = CacheManager.create();
        cacheManager.addCache(ehCache);

        lifecycle.addListener(LifecycleStage.STOP_STAGE, new LifecycleListener() {
            @Override
            public void onStage(LifecycleStage lifecycleStage) {
                cacheManager.shutdown();
            }
        });
    }

    @Override
    public void set(String namespace, Map<String, ? extends DataProvider<byte []>> stores) {
        for (Entry<String, ? extends DataProvider<byte []>> e : stores.entrySet()) {
            ehCache.put(new Element(
                    makeKey(namespace, e.getKey()),
                    e.getValue()));
        }
    }

    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys) {
        ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
        for (String key : keys) {
            Element value = ehCache.get(makeKey(namespace, key));

            if (value != null && value.getObjectValue() != null) {
                @SuppressWarnings("unchecked")
                DataProvider<byte []> storedEntry = (DataProvider<byte []>)value.getObjectValue();
                if (storedEntry.getExpiry().isAfterNow()) {
                    builder.put(key, storedEntry.getData());
                } else {
                    clear(namespace, Collections.singleton(key));
                }
            }

        }
        return builder.build();
    }

    @Override
    public void clear(String namespace, Collection<String> keys) {
        for (String key : keys) {
            ehCache.remove(makeKey(namespace, key));
        }
    }

    private Entry<String, String> makeKey(String namespace, String key) {
        return Maps.immutableEntry(namespace, key);
    }
}
