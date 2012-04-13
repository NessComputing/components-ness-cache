package ness.cache2;

import com.nesscomputing.logging.Log;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;
import com.google.inject.Singleton;

/** An in JVM cache that does not evict its keys due to memory pressure.
 *
 * Note: once ehcache 2.5 is released this can be implemented as a subclass of JvmCacheProvider with pinning in ehcache.
 *
 * @author christopher
 *
 */
@Singleton
public class NonEvictingJvmCacheProvider implements InternalCacheProvider {
	private final static Log LOG = Log.findLog();
	private final ConcurrentMap<Map.Entry<String, String>, byte[]> map = Maps.newConcurrentMap();

	@Override
	public void set(String namespace, Collection<CacheStore<byte []>> stores) {
		for (CacheStore<byte []> entry: stores) {
			LOG.trace("%s setting %s:%s", this, namespace, entry.getKey());
            Entry<String, String> key = Maps.immutableEntry(namespace, entry.getKey());
			byte[] value = entry.getData();
			if (value != null) {
                map.put(key, value);
			} else {
			    map.remove(key);
			}
		}
	}

	@Override
	public Map<String, byte[]> get(String namespace, Collection<String> keys) {
		Map<String, byte[]> ret = Maps.newHashMap();
		for (String key: keys) {
			byte[] data = map.get(Maps.immutableEntry(namespace, key));
			LOG.trace("%s getting %s:%s=%s", this, namespace, key, data);
			if (data != null) {
			    ret.put(key, data);
			}
		}
		return ret;
	}

	@Override
	public void clear(String namespace, Collection<String> keys) {
		for (String key: keys) {
			LOG.trace("%s clearing %s:%s", this, namespace, key);
			map.remove(Maps.immutableEntry(namespace, key));
		}
	}

    @Override
    public Map<String, Boolean> add(String namespace, Collection<CacheStore<byte []>> stores)
    {
        final Map<String, Boolean> result = Maps.newHashMap();

        for (CacheStore<byte []> entry: stores) {
            LOG.trace("%s setting %s:%s", this, namespace, entry.getKey());
            Entry<String, String> key = Maps.immutableEntry(namespace, entry.getKey());
            byte[] data = entry.getData();
            byte[] old;
            if (data != null) {
                old = map.putIfAbsent(key, data);
            } else {
                old = null;
            }
            result.put(entry.getKey(), old == null);
        }
        return result;
    }
}
