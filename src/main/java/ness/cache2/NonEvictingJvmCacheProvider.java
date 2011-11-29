package ness.cache2;

import io.trumpet.log.Log;

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
	private final Map<Map.Entry<String, String>, byte[]> map = Maps.newConcurrentMap();

	@Override
	public void set(String namespace, Map<String, ? extends DataProvider<byte []>> stores) {
		for (Map.Entry<String, ? extends DataProvider<byte []>> entry: stores.entrySet()) {
			LOG.trace("%s setting %s:%s", this, namespace, entry.getKey());
			map.put(Maps.immutableEntry(namespace, entry.getKey()), entry.getValue().getData());
		}
	}

	@Override
	public Map<String, byte[]> get(String namespace, Collection<String> keys) {
		Map<String, byte[]> ret = Maps.newHashMap();
		for (String key: keys) {
			byte[] data = map.get(Maps.immutableEntry(namespace, key));
			LOG.trace("%s getting %s:%s=%s", this, namespace, key, data);
			ret.put(key, data);
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
}
