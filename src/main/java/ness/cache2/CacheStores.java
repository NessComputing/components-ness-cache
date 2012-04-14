package ness.cache2;

import java.util.Arrays;
import java.util.Collection;

import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;

@edu.umd.cs.findbugs.annotations.SuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
public final class CacheStores
{
    private CacheStores()
    {
    }

    /**
     * Create a new cache entry ready for storing
     * @param data the data to store; this data is shared (not copied) for efficiency and should never be modified after being handed off.
     * @param expiry the expiration instant; this is advisory and cache entries may expire sooner (or later, in certain circumstances)
     */
    static CacheStore<byte []> fromSharedBytes(final String key, byte[] data, DateTime expiry) {
        return new CacheStore<byte []>(key, data, expiry);
    }

    /**
     * Create a new cache entry ready for storing
     * @param data the data to store; this data is copied and may be modified after the invocation completes
     * @param expiry the expiration instant; this is advisory and cache entries may expire sooner (or later, in certain circumstances)
     */
    static CacheStore<byte []> fromClonedBytes(final String key, byte[] data, DateTime expiry) {
        return new CacheStore<byte []>(key, Arrays.copyOf(data, data.length), expiry);
    }

    static Collection<CacheStore<Void>> forKeys(final Collection<String> keys, final DateTime expiry)
    {
        return Collections2.transform(keys, new Function<String, CacheStore<Void>>() {

            @Override
            @edu.umd.cs.findbugs.annotations.SuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
            public CacheStore<Void> apply(final String key) {
                Preconditions.checkArgument(key != null, "null key");
                return new CacheStore<Void>(key, null, expiry);
            }

        });
    }

    static Collection<CacheStore<Integer>> forKeys(final Collection<String> keys, final int value, final DateTime expiry)
    {
        return Collections2.transform(keys, new Function<String, CacheStore<Integer>>() {

            @Override
            @edu.umd.cs.findbugs.annotations.SuppressWarnings("NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE")
            public CacheStore<Integer> apply(final String key) {
                Preconditions.checkArgument(key != null, "null key");
                return new CacheStore<Integer>(key, value, expiry);
            }

        });
    }
}
