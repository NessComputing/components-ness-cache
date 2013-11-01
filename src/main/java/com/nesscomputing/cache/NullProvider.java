package com.nesscomputing.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Basic null cache.  All stores are ignored, all fetches return no entries.
 */
public class NullProvider implements InternalCacheProvider {
    @Override
    public void set(String namespace, Collection<CacheStore<byte []>> stores, @Nullable CacheStatistics statistics) { }

    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys, @Nullable CacheStatistics statistics) {
        return Collections.emptyMap();
    }

    @Override
    public void clear(String namepsace, Collection<String> keys, @Nullable CacheStatistics statistics) { }

    @Override
    public Map<String, Boolean> add(final String namespace, final Collection<CacheStore<byte []>> stores, @Nullable CacheStatistics statistics)
    {
        return Collections.emptyMap();
    }
}
