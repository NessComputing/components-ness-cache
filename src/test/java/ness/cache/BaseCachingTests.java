package ness.cache;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.Test;

import com.google.inject.Inject;

public abstract class BaseCachingTests {
    @Inject
    Cache cache;
    
    @Test
    public void testSimpleCache() {
        String ns = "a";
        Collection<String> b = Collections.singleton("b");
        Map<String, byte[]> fetch = cache.get(ns, b);
        assertTrue(fetch.toString(), fetch.isEmpty());
        cache.set(ns, Collections.singletonMap("b", new CacheStore(new byte[] { 42 }, new DateTime().plusMinutes(1))));
        fetch = cache.get(ns, b);
        assertArrayEquals(fetch.toString(), new byte[] { 42 } , fetch.get("b"));
        cache.clear(ns, b);
        fetch = cache.get(ns, b);
        assertTrue(fetch.toString(), fetch.isEmpty());
    }
    
    @Test
    public void testNamespacing() {
        Collection<String> b = Collections.singleton("b");
        assertTrue(cache.get("a", b).isEmpty());
        cache.set("a", Collections.singletonMap("b", new CacheStore(new byte[] { 42 }, new DateTime().plusMinutes(1))));
        assertArrayEquals(new byte[] { 42 } , cache.get("a", b).get("b"));
        cache.clear("b", b);
        assertArrayEquals(new byte[] { 42 } , cache.get("a", b).get("b"));
        assertTrue(cache.get("b", b).isEmpty());
    }
    
    @Test
    public void testNamedCache() {
        NamespacedCache namedCache = cache.withNamespace("test");
        
        assertNull(namedCache.get("x"));
        assertNull(namedCache.get("y"));
        namedCache.set("x", new byte[] { 69 }, new DateTime().plusMinutes(1));
        assertArrayEquals(new byte[] { 69 }, namedCache.get("x"));
        assertNull(namedCache.get("y"));
        assertTrue(cache.get("test2", Collections.singleton("x")).isEmpty());
    }
}
