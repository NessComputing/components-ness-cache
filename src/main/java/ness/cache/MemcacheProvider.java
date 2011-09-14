package ness.cache;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trumpet.log.Log;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provide a cache based upon a memached server
 */
@Singleton
final class MemcacheProvider implements InternalCacheProvider {
    private static final Log LOG = Log.findLog();
    private static final Pattern SPECIFIC_KEY_PATTERN = Pattern.compile("^([^\u0001]*)\u0001([^\u0001]*)$");
    private final MemcachedClientFactory clientFactory;
    private final CacheConfiguration config;
    private final ThreadLocal<Base64> base64ThreadLocal;

    @Inject
    MemcacheProvider(CacheConfiguration config, MemcachedClientFactory clientFactory) {
        this.config = config;
        this.clientFactory = clientFactory;
        this.base64ThreadLocal = new ThreadLocal<Base64>() {
            protected Base64 initialValue() {
                return new Base64();
            }
        };
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

            transformedResults.put(getSpecificKeyFromEncodedNSKey(e.getKey()), e.getValue());
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
        Base64 base64 = base64ThreadLocal.get();
        final String namespaceb64 = base64.encodeAsString(namespace.getBytes(Charsets.UTF_8));
        return Collections2.transform(keys, new Function<String, String>() {
            @Override
            public String apply(String key) {
                return makeKeyBulk(namespaceb64, key);
            }
        });
    }

    private String makeKey(String namespace, String key) {
        Base64 base64 = base64ThreadLocal.get();
        String namespaceKey = base64.encodeAsString(namespace.getBytes(Charsets.UTF_8));
        String specificKey = base64.encodeAsString(key.getBytes(Charsets.UTF_8));

        return namespaceKey + "\u0001" + specificKey;
    }

    private String makeKeyBulk(String b64namespace, String key) {
        Base64 base64 = base64ThreadLocal.get();
        String specificKey = base64.encodeAsString(key.getBytes(Charsets.UTF_8));

        return b64namespace + "\u0001" + specificKey;
    }

    private String getSpecificKeyFromEncodedNSKey(String encodedKey) {
        Matcher m = SPECIFIC_KEY_PATTERN.matcher(encodedKey);
        if (!m.find()) {
            throw new IllegalStateException("makeKey changed but not SPECIFIC_KEY_PATTERN; fixme");
        }

        Base64 base64 = base64ThreadLocal.get();
        return new String(base64.decode(m.group(2)), Charsets.UTF_8);
    }
}
