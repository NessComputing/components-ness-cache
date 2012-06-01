package ness.cache2;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.ConfigurationFactory;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.nesscomputing.lifecycle.Lifecycle;
import com.nesscomputing.lifecycle.LifecycleListener;
import com.nesscomputing.lifecycle.LifecycleStage;

/**
 * An in-JVM cache, currently backed by EHCache
 */
@Singleton
public class JvmCacheProvider implements InternalCacheProvider {

    private final Cache ehCache;

    @Inject
    JvmCacheProvider(Lifecycle lifecycle) {
        ehCache = new Cache(new CacheConfiguration("ness.cache." + hashCode(), 100000)
                .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LFU)
                .overflowToDisk(false)
                .diskPersistent(false));

        final Configuration ehcacheConfig = ConfigurationFactory.parseConfiguration();
        ehcacheConfig.setUpdateCheck(false);
        final CacheManager cacheManager = CacheManager.create(ehcacheConfig);

        cacheManager.addCache(ehCache);

        lifecycle.addListener(LifecycleStage.STOP_STAGE, new LifecycleListener() {
            @Override
            public void onStage(LifecycleStage lifecycleStage) {
                cacheManager.shutdown();
            }
        });
    }

    @Override
    public void set(String namespace, Collection<CacheStore<byte []>> stores) {
        for (CacheStore<byte []> e : stores) {
            ehCache.put(new Element(
                    makeKey(namespace, e.getKey()),
                    e));
        }
    }

    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys) {
        ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
        for (String key : keys) {
            Element value = ehCache.get(makeKey(namespace, key));

            if (value != null && value.getObjectValue() != null) {
                @SuppressWarnings("unchecked")
                CacheStore<byte []> storedEntry = (CacheStore<byte []>)value.getObjectValue();
                final DateTime expiry = storedEntry.getExpiry();
                final byte [] data = storedEntry.getData();
                if ((expiry == null || expiry.isAfterNow()) && data != null) {
                    builder.put(key, data);
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

    @Override
    public Map<String, Boolean> add(final String namespace, final Collection<CacheStore<byte []>> stores)
    {
        final Map<String, Boolean> resultMap = Maps.newHashMap();
        for (CacheStore<byte []> e : stores) {
            final Element old = ehCache.putIfAbsent(new Element(makeKey(namespace, e.getKey()), e));
            resultMap.put(e.getKey(), old == null);
        }
        return resultMap;
    }

    private Entry<String, String> makeKey(String namespace, String key) {
        return Maps.immutableEntry(namespace, key);
    }
}
