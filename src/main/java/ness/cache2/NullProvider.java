package ness.cache2;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Basic null cache.  All stores are ignored, all fetches return no entries.
 */
public class NullProvider implements InternalCacheProvider {
    @Override
    public void set(String namespace, Collection<CacheStore<byte []>> stores) { }

    @Override
    public Map<String, byte[]> get(String namespace, Collection<String> keys) {
        return Collections.emptyMap();
    }

    @Override
    public void clear(String namepsace, Collection<String> keys) { }

    public Map<String, Boolean> add(final String namespace, final Collection<CacheStore<byte []>> stores)
    {
        return Collections.emptyMap();
    }
}
