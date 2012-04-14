package ness.cache2.guava;

import org.joda.time.Duration;

import com.google.common.base.Function;
import com.google.common.cache.CacheLoader;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;

/**
 * Guice module builder which provides Cache&lt;K, V&gt; implementations for any
 * type K, V backed by NessCache.
 *
 * <p>Supports custom serialization and deserialization, and randomized expiration dates.
 *
 * <p>Unless custom Function implementations are provided, the builder will attempt to find
 * serializers that have been bound in the current Guice injector.  It will look for bindings to
 * <ul>
 * <li><code>Function&lt;? super K, String&gt;</code> for key serialization</li>
 * <li><code>Function&lt;? super V, byte[]&gt;</code> for value serialization</li>
 * <li><code>Function&lt;byte[], ? extends V&gt;</code> for value deserialization</li>
 * </ul>
 *
 * <p>These are conveniently the same bindings produced by a <code>JacksonSerializerBinder</code> if you want
 * to map the keys or values to JSON.
 *
 * <p>If no configuration of key or value types are provided, the default <code>Cache&lt;String, byte[]&gt;</code> has
 * identity serializers.
 *
 * <p>Instances are immutable, so method calls <b>must</b> be chained to be effective.
 */
public interface GuavaCacheModuleBuilder<K, V> {
    /**
     * Set the key type for this Cache.  Any prior key serialization configuration is lost and replaced
     * by looking up the default according to the rules as specified on the class documentation.
     */
    <NewK> GuavaCacheModuleBuilder<NewK, V> withKeyType(Class<NewK> kClass);

    /**
     * Set the key type for this Cache.  Any prior key serialization configuration is lost and replaced
     * by looking up the default according to the rules as specified on the class documentation.
     */
    <NewK> GuavaCacheModuleBuilder<NewK, V> withKeyType(TypeLiteral<NewK> kClass);

    /**
     * Specify the key serialization function.  Prevents lookup in Guice.
     */
    GuavaCacheModuleBuilder<K, V> withKeySerializer(Function<? super K, String> keySerialization);

    /**
     * Set the value type for this Cache.  Any prior value serialization configuration is lost and replaced
     * by looking up the default according to the rules as specified on the class documentation.
     */
    <NewV> GuavaCacheModuleBuilder<K, NewV> withValueType(Class<NewV> vClass);

    /**
     * Set the value type for this Cache.  Any prior value serialization configuration is lost and replaced
     * by looking up the default according to the rules as specified on the class documentation.
     */
    <NewV> GuavaCacheModuleBuilder<K, NewV> withValueType(TypeLiteral<NewV> vClass);

    /**
     * Set up the value serialization and deserialization functions.  Prevents lookup in Guice.
     */
    GuavaCacheModuleBuilder<K, V> withValueSerializer(
            Function<? super V, byte[]> valueSerializer,
            Function<byte[], ? extends V> valueDeserializer);

    /**
     * Configure the expiration duration
     */
    GuavaCacheModuleBuilder<K, V> withExpiration(Duration expiry);

    /**
     * Configure the expiration duration and add a random amount from <code>-jitter</code> to <code>+jitter</code>,
     * distributed approximately evenly
     */
    GuavaCacheModuleBuilder<K, V> withExpiration(Duration expiry, Duration expiryJitter);

    /**
     * Configure and return this module with no cache loader configured
     */
    Module build();

    /**
     * Configure and return this module with a cache loader supplied
     */
    Module build(final CacheLoader<? super K, ? extends V> cacheLoader);

    /**
     * Configure and return this module with a Provider for the cache loader supplied
     */
    Module build(final Provider<? extends CacheLoader<? super K, ? extends V>> cacheLoaderProvider);

    /**
     * Configure and return this module with the cache loader injected from the given key
     */
    Module build(final Key<CacheLoader<? super K, ? extends V>> key);
}
