package ness.cache;

import java.util.Arrays;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;

/**
 * Encapsulates a single store of arbitrary data, which expires at a given time.
 */
@NotThreadSafe
public class CacheStore {
    private final byte[] data;
    private final DateTime expiry;

    /**
     * Create a new cache entry ready for storing
     * @param data the data to store; this data is shared (not copied) for efficiency and should never be modified after being handed off.
     * @param expiry the expiration instant; this is advisory and cache entries may expire sooner (or later, in certain circumstances)
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="efficiency")
    CacheStore(byte[] data, DateTime expiry) {
        Preconditions.checkNotNull(data);
        Preconditions.checkNotNull(expiry);
        Preconditions.checkArgument(expiry.isAfterNow(), "expiry time in the past");
        this.data = data;
        this.expiry = expiry;
    }

    /**
     * Create a new cache entry ready for storing
     * @param data the data to store; this data is shared (not copied) for efficiency and should never be modified after being handed off.
     * @param expiry the expiration instant; this is advisory and cache entries may expire sooner (or later, in certain circumstances)
     */
    public static CacheStore fromSharedBytes(byte[] data, DateTime expiry) {
        return new CacheStore(data, expiry);
    }

    /**
     * Create a new cache entry ready for storing
     * @param data the data to store; this data is copied and may be modified after the invocation completes
     * @param expiry the expiration instant; this is advisory and cache entries may expire sooner (or later, in certain circumstances)
     */
    public static CacheStore fromClonedBytes(byte[] data, DateTime expiry) {
        return new CacheStore(Arrays.copyOf(data, data.length), expiry);
    }

    /**
     * @return the advisory cache entry expiry time.
     */
    @Nonnull
    public DateTime getExpiry() {
        return expiry;
    }

    /**
     * @return the data for this cache entry; this is shared and must not be modified via this reference.
     */
    @Nonnull
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="efficiency")
    public byte[] getBytes() {
        return data;
    }
}
