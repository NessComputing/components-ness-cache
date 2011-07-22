package ness.cache;

import io.trumpet.log.Log;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import net.spy.memcached.MemcachedClient;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Provide a cache based upon a memached server
 */
@Singleton
final class MemcacheProvider implements InternalCacheProvider {
    private static final Log LOG = Log.findLog();
    private final MemcachedClientFactory clientFactory;
    private final CacheConfiguration config;

    @Inject
    MemcacheProvider(CacheConfiguration config, MemcachedClientFactory clientFactory) {
        this.config = config;
        this.clientFactory = clientFactory;
    }

    @Override
    public void set(String namespace, Map<String, CacheStore> stores) {
        MemcachedClient client = clientFactory.get();
        if (client == null) {
            return;
        }

        for (final Entry<String, CacheStore> action : stores.entrySet()) {

            CacheStore cacheStore = action.getValue();
            String preparedKey = makeKey(namespace, action.getKey());
            int expiry = computeMemcacheExpiry(cacheStore.getExpiry());
            byte[] bytes = cacheStore.getBytes();
            
            Future<Boolean> future;
            try {
                future = client.set(preparedKey, expiry, bytes);
            } catch (Exception e) {
                LOG.error(e, "Spymemcached tossed an exception while storing %s", preparedKey);
                continue;
            }

            if (config.isCacheSynchronous()) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    LOG.error(e, "Exception while setting cache entry %s %s", namespace, action);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys) {
        MemcachedClient client = clientFactory.get();
        if (client == null) {
            return Collections.emptyMap();
        }

        assert MemcacheByteArrayTranscoder.class.isAssignableFrom(client.getTranscoder().getClass());

        Collection<String> preparedKeys = makeKeys(namespace, keys);
        final Map<String, byte[]> result;
        final Map<String, Object> internalResult;
        try {
            // The assertion above protects this somewhat dodgy-looking cast.  Since the transcoder always
            // returns byte[], it is safe to cast the value Object to byte[].
            internalResult = client.getBulk(preparedKeys);
        } catch (Exception e) {
            LOG.error(e, "Spymemcached tossed an exception");
            return Collections.emptyMap();
        }
        result = (Map<String, byte[]>) (Map<String, ?>) internalResult;

        ImmutableMap.Builder<String, byte[]> transformedResults = ImmutableMap.builder();

        for (Entry<String, byte[]> e : result.entrySet()) {
            if (e.getValue() == null) {
                continue;
            }

            transformedResults.put(StringUtils.removeStart(e.getKey(), makeKey(namespace, "")), e.getValue());
        }

        return transformedResults.build();
    }

    @Override
    public void clear(String namespace, Collection<String> keys) {
        MemcachedClient client = clientFactory.get();
        if (client == null) {
            return;
        }
        
        for (String key : keys) {
            String preparedKey = makeKey(namespace, key);
            Future<Boolean> future;
            try {
                future = client.delete(preparedKey);
            } catch (Exception e) {
                LOG.error(e, "Spymemcached tossed an exception while clearing %s", key);
                continue;
            }

            if (config.isCacheSynchronous()) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    LOG.error(e, "Exception while clearing cache entry %s %s", namespace, key);
                }
            }
        }
    }

    /** Memcache expects expiration dates in seconds since the epoch */
    protected int computeMemcacheExpiry(DateTime when) {
        return Ints.saturatedCast(when.getMillis() / 1000);
    }

    private Collection<String> makeKeys(final String namespace, Collection<String> keys) {
        return Collections2.transform(keys, new Function<String, String>() {
            @Override
            public String apply(String key) {
                return makeKey(namespace, key);
            }
        });
    }

    private String makeKey(String namespace, String key) {
        return namespace + "\u0001" + key;
    }
}
