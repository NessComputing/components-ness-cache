package ness.cache2;

import java.util.Arrays;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.ObjectUtils;
import org.joda.time.DateTime;

import com.google.common.base.Preconditions;

/**
 * Encapsulates a single store of arbitrary data, which expires at a given time.
 */
@NotThreadSafe
public class CacheStore<D> {
    private final String key;
    private final D data;
    private final DateTime expiry;

    /**
     * Create a new cache entry ready for storing
     * @param data the data to store; this data is shared (not copied) for efficiency and should never be modified after being handed off.
     * @param expiry the expiration instant; this is advisory and cache entries may expire sooner (or later, in certain circumstances)
     */
    @SuppressWarnings("EI_EXPOSE_REP")
    CacheStore(@Nonnull final String key,
               @Nullable final D data,
               @Nullable DateTime expiry) {
        Preconditions.checkNotNull(key);

        if (expiry != null) {
            Preconditions.checkArgument(expiry.isAfterNow(), "expiry time in the past");
        }

        this.key = key;
        this.data = data;
        this.expiry = expiry;
    }

    /**
     * Returns the store key.
     */
    @Nonnull
    public String getKey()
    {
        return key;
    }

    /**
     * @return the advisory cache entry expiry time.
     */
    @CheckForNull
    public DateTime getExpiry() {
        return expiry;
    }

    /**
     * @return the data for this cache entry; this is shared and must not be modified via this reference.
     */
    @CheckForNull
    @SuppressWarnings("EI_EXPOSE_REP")
    public D getData() {
        return data;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CacheStore [");
        if (key != null)
            builder.append("key=").append(key).append(", ");
        if (data != null)
            builder.append("data=").append(prettyPrintData()).append(", ");
        if (expiry != null)
            builder.append("expiry=").append(expiry);
        builder.append("]");
        return builder.toString();
    }

    private String prettyPrintData() {
        if (data instanceof byte[]) {
            return Arrays.toString((byte[]) data);
        }
        return ObjectUtils.toString(data);
    }
}
