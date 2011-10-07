package ness.cache2;

import io.trumpet.log.Log;
import io.trumpet.util.Pair;

import java.util.Collection;
import java.util.Map;

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
	private final Map<Pair<String, String>, byte[]> map = Maps.newConcurrentMap();

	@Override
	public void set(String namespace, Map<String, CacheStore> stores) {
		for (Map.Entry<String, CacheStore> entry: stores.entrySet()) {
			LOG.trace("%s setting %s:%s", this, namespace, entry.getKey());
			map.put(Pair.of(namespace, entry.getKey()), entry.getValue().getBytes());
		}
	}

	@Override
	public Map<String, byte[]> get(String namespace, Collection<String> keys) {
		Map<String, byte[]> ret = Maps.newHashMap();
		for (String key: keys) {
			byte[] data = map.get(Pair.of(namespace, key));
			LOG.trace("%s getting %s:%s=%s", this, namespace, key, data);
			ret.put(key, data);
		}
		return ret;
	}

	@Override
	public void clear(String namespace, Collection<String> keys) {
		for (String key: keys) {
			LOG.trace("%s clearing %s:%s", this, namespace, key);
			map.remove(Pair.of(namespace, key));
		}
	}

}
