package ness.cache;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class NullProvider implements InternalCacheProvider {
    @Override
    public void set(String namepsace, Map<String, CacheStore> stores) { }

    @Override
    public Map<String, byte[]> get(String namepsace, Collection<String> keys) {
        return Collections.emptyMap();
    }

    @Override
    public void clear(String namepsace, Collection<String> keys) { }
}
