package ness.cache;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
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
    private static final String SEPARATOR = "\u0001";
    private static final Pattern SPECIFIC_KEY_PATTERN = Pattern.compile(
        "^([^\\" + SEPARATOR + "]*)\\Q"
            + SEPARATOR
            + "\\E([^\\" + SEPARATOR + "]*)$");
    private final MemcachedClientFactory clientFactory;
    private final CacheConfiguration config;
    private final Function<String,String> encoder;
    private final Function<String,String> decoder;

    @Inject
    MemcacheProvider(CacheConfiguration config, MemcachedClientFactory clientFactory) {
        this.config = config;
        this.clientFactory = clientFactory;
        CacheConfiguration.EncodingType encodingType = config.getMemcachedEncoding();

        switch(encodingType) {
        case BASE64:
            final ThreadLocal<Base64> base64ThreadLocal =  new ThreadLocal<Base64>() {
                protected Base64 initialValue() {
                    return new Base64();
                }
            };
            encoder = new Function<String, String>() {
                @Override
                public String apply(String input) {
                    return base64ThreadLocal.get().encodeAsString(input.getBytes(Charsets.UTF_8));
                }
            };
            decoder = new Function<String, String>() {
                @Override
                public String apply(String input) {
                    return new String(base64ThreadLocal.get().decode(input), Charsets.UTF_8);
                }
            };
            break;
        case NONE:
            this.encoder = Functions.identity();
            this.decoder = Functions.identity();
            break;
        default:
            throw new IllegalArgumentException("Unknown encoding type " + encodingType);
        }
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

            transformedResults.put(getSpecificKeyFromNSKey(e.getKey()), e.getValue());
        }

        return transformedResults.build();
    }

    @Override
    public void clear(String namespace, Collection<String> keys) {
        MemcachedClient client = clientFactory.get();
        if (client == null) {
            return;
        }

        String nsEncoded = encoder.apply(namespace);

        for (String key : keys) {
            String preparedKey = makeKeyWithNamespaceAlreadyEncoded(nsEncoded, key);
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
        final String namespaceEncoded = encoder.apply(namespace);
        return Collections2.transform(keys, new Function<String, String>() {
            @Override
            public String apply(String key) {
                return makeKeyWithNamespaceAlreadyEncoded(namespaceEncoded, key);
            }
        });
    }

    private String makeKey(String namespace, String key) {
        namespace = encoder.apply(namespace);
        key = encoder.apply(key);

        return namespace + SEPARATOR + key;
    }

    private String makeKeyWithNamespaceAlreadyEncoded(String encodedNamespace, String key) {
        return encodedNamespace + SEPARATOR + encoder.apply(key);
    }

    private String getSpecificKeyFromNSKey(String encodedKey) {
        Matcher m = SPECIFIC_KEY_PATTERN.matcher(encodedKey);
        if (!m.find()) {
            throw new IllegalStateException("makeKey changed but not SPECIFIC_KEY_PATTERN; fixme");
        }

        return decoder.apply(m.group(2));
    }
}
