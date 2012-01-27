package ness.cache2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.inject.Inject;
import com.google.inject.name.Named;

public abstract class BaseCachingTests {
    @Inject
    @Named("test")
    Cache cache;

    @Test
    public void testSimpleCache() {
        String ns = "a";
        Collection<String> b = Collections.singleton("b");
        Map<String, byte[]> fetch = cache.get(ns, b);
        assertTrue(fetch.toString(), fetch.isEmpty());
        cache.set(ns, Collections.singleton(new CacheStore<byte []>("b", new byte[] { 42 }, new DateTime().plusMinutes(1))));
        fetch = cache.get(ns, b);
        assertArrayEquals(fetch.toString(), new byte[] { 42 } , fetch.get("b"));
        cache.clear(ns, b);
        fetch = cache.get(ns, b);
        assertTrue(fetch.toString(), fetch.isEmpty());
    }

    @Test
    public void testUnprintables() {
        String ns = "\u0002,\u0005::!";
        String key = "\u0003 - \u0004!\n\n\n\u0009";
        Collection<String> b = Collections.singleton(key);

        Map<String, byte[]> fetch = cache.get(ns, b);
        assertTrue(fetch.toString(), fetch.isEmpty());
        cache.set(ns, Collections.singleton(new CacheStore<byte []>(key, new byte[] { 42 }, new DateTime().plusMinutes(1))));
        fetch = cache.get(ns, b);
        assertArrayEquals(fetch.toString(), new byte[] { 42 } , fetch.get(key));
        cache.clear(ns, b);
        fetch = cache.get(ns, b);

        assertTrue(fetch.toString(), fetch.isEmpty());
    }
    @Test
    public void testNamespacing() {
        Collection<String> b = Collections.singleton("b");
        assertTrue(cache.get("a,", b).isEmpty());
        cache.set("a,", Collections.singleton(new CacheStore<byte []>("b", new byte[] { 42 }, new DateTime().plusMinutes(1))));
        assertArrayEquals(new byte[] { 42 } , cache.get("a,", b).get("b"));
        cache.clear("b", b);
        assertArrayEquals(new byte[] { 42 } , cache.get("a,", b).get("b"));
        assertTrue(cache.get("b", b).isEmpty());
    }

    @Test
    public void testNamedCache() {
        NamespacedCache namedCache = cache.withNamespace("test");

        assertNull(namedCache.get("x z "));
        assertNull(namedCache.get("y"));
        namedCache.set("x z ", new byte[] { 69 }, new DateTime().plusMinutes(1));
        assertArrayEquals(new byte[] { 69 }, namedCache.get("x z "));
        assertNull(namedCache.get("y"));
        assertTrue(cache.get("test2", Collections.singleton("x z ")).isEmpty());
    }

    @Test
    public void testAddOperation() throws Exception
    {
        final byte [] bytes = "foo".getBytes(Charsets.UTF_8);
        final NamespacedCache namedCache = cache.withNamespace("test");

        assertNull(namedCache.get("y"));

        final Boolean result = namedCache.add("y", bytes, null);
        Assert.assertNotNull(result);
        Assert.assertTrue(result);

        Assert.assertArrayEquals(bytes, namedCache.get("y"));


        final Boolean result2 = namedCache.add("y", bytes, null);
        Assert.assertNotNull(result2);
        // Second add should not succeed.
        Assert.assertFalse(result2);
    }
}
