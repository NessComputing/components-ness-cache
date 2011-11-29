package ness.cache2;

import io.trumpet.log.Log;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.joda.time.DateTime;

import net.spy.memcached.MemcachedClient;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.google.inject.Singleton;


/**
 * Provide a cache based upon a memached server
 */
@Singleton
final class MemcacheProvider implements InternalCacheProvider {

    private static final Function<String, String> BASE64_ENCODER = new Function<String, String>() {
        @Override
        public String apply(final String input) {
            return new String(Base64.encode(input), Charsets.UTF_8);
        }
    };

    private static final Function<String, String> BASE64_DECODER = new Function<String, String>() {
        @Override
        public String apply(final String input) {
            return new String(Base64.decode(input), Charsets.UTF_8);
        }
    };

    private static final FetchingCallback<Boolean, byte[]> ADD_CALLBACK = new FetchingCallback<Boolean, byte[]>() {
        @Override
        public Future<Boolean> callback(final MemcachedClient client, final String key, final int expiry, final byte [] data) throws InterruptedException {
            return client.add(key, expiry, data);
        }
    };

    private static final FetchingCallback<Boolean, byte[]> SET_CALLBACK = new FetchingCallback<Boolean, byte[]>() {
        @Override
        public Future<Boolean> callback(final MemcachedClient client, final String key, final int expiry, final byte [] data) throws InterruptedException {
            return client.set(key, expiry, data);
        }
    };

    private static final OpCallback<Boolean> CLEAR_CALLBACK = new FetchingCallback<Void, Void>() {
        @Override
        public Future<Boolean> callback(final MemcachedClient client, final String key) throws InterruptedException {
            return client.delete(key);
        }
    };


    private static final Log LOG = Log.findLog();
    private static final String SEPARATOR = "\u0001";

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
            this.encoder = BASE64_ENCODER;
            this.decoder = BASE64_DECODER;
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
    public void set(final String namespace, final Map<String, ? extends DataProvider<byte []>> stores)
    {
        op(namespace, stores, SET_CALLBACK, false);
    }

    public Map<String, Boolean> add(final String namespace, final Map<String, ? extends DataProvider<byte []>> stores)
    {
        return op(namespace, stores, ADD_CALLBACK, true);
    }

    @Override
    public Map<String, Boolean> clear(final String namespace, final Collection<String> keys)
    {
        return op(namespace, keys, CLEAR_CALLBACK);
    }

    private <T, U> Map<String, T> op(final String namespace, final Map<String, ? extends DataProvider<U>> stores, final FetchingCallback<T, U> callback, final boolean wait)
    {
        final MemcachedClient client = clientFactory.get();

        if (client == null) {
            return Collections.emptyMap();
        }

        final Map<String, Future<T>> futures = Maps.newHashMap();
        final Map<String, T> results = Maps.newHashMap();

        try {
            for (final Entry<String, ? extends DataProvider<U>> action : stores.entrySet()) {

                final DataProvider<U> dataProvider = action.getValue();
                final String key = action.getKey();

                Future<T> future = null;
                try {
                    future = callback.callback(client,
                                               makeKey(namespace, key),
                                               computeMemcacheExpiry(dataProvider.getExpiry()),
                                               dataProvider.getData()); // client.add(preparedKey, expiry, bytes);
                    futures.put(key, future);
                } catch (IllegalStateException ise) {
                    LOG.errorDebug(ise, "Memcache Queue was full while storing %s:%s", namespace, key);
                }

                syncCheck(future, namespace, action);
            }

            if (wait) {
                for (Map.Entry<String, Future<T>> entry : futures.entrySet()) {
                    try {
                        results.put(entry.getKey(), entry.getValue().get());
                    } catch (ExecutionException e) {
                        LOG.errorDebug(e.getCause(), "Cache entry %s:%s", namespace, entry.getKey());
                    }
                }
            }
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return results;
    }

    private <T> Map<String, T> op(final String namespace, final Collection<String> keys, final OpCallback<T> callback, final boolean wait)
    {
        final MemcachedClient client = clientFactory.get();

        if (client == null) {
            return Collections.emptyMap();
        }

        final Map<String, Future<T>> futures = Maps.newHashMap();
        final Map<String, T> results = Maps.newHashMap();

        try {
            for (final String key : keys) {
                Future<T> future = null;
                try {
                    future = callback.callback(client, makeKey(namespace, key));
                    futures.put(key, future);
                } catch (IllegalStateException ise) {
                    LOG.errorDebug(ise, "Memcache Queue was full while storing %s:%s", namespace, key);
                }

                syncCheck(future, namespace, action);
            }

            if (wait) {
                for (Map.Entry<String, Future<T>> entry : futures.entrySet()) {
                    try {
                        results.put(entry.getKey(), entry.getValue().get());
                    } catch (ExecutionException e) {
                        LOG.errorDebug(e.getCause(), "Cache entry %s:%s", namespace, entry.getKey());
                    }
                }
            }
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return results;
    }

    private void syncCheck(final Future<?> future, final String namespace, final Entry<String, ? extends DataProvider<?>> action)
        throws InterruptedException
    {
        if (future != null && config.isCacheSynchronous()) {
            try {
                future.get();
            } catch (ExecutionException e) {
                LOG.errorDebug(e.getCause(), "Cache entry %s:%s", namespace, action.getKey());
            }
        }
    }

    public interface FetchingCallback<T, U>
    {
        Future<T> callback(MemcachedClient client, String key, int expiry, U data) throws InterruptedException;
    }

    public interface OpCallback<T>
    {
        Future<T> callback(MemcachedClient client, String key) throws InterruptedException;
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

        String encodedNamespacePlusSeparator = encoder.apply(namespace) + SEPARATOR;
        int prefixLength = encodedNamespacePlusSeparator.length();

        for (Entry<String, byte[]> e : result.entrySet()) {
            if (e.getValue() == null) {
                continue;
            }

            transformedResults.put(decoder.apply(e.getKey().substring(prefixLength)), e.getValue());
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
}
