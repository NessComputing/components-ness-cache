/**
 * Copyright (C) 2012 Ness Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nesscomputing.cache.guava;

import java.lang.annotation.Annotation;

import org.joda.time.Duration;

import com.google.common.base.Function;
import com.google.common.cache.CacheLoader;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;

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
 */
public interface GuavaCacheModuleBuilder<K, V> {

    /**
     * Use the default (Guice bound) serialization function for both key and value
     */
    GuavaCacheModuleBuilder<K, V> withSerializers();

    /**
     * Specify the key and value serialization function binding annotation.
     */
    GuavaCacheModuleBuilder<K, V> withSerializers(Annotation bindingAnnotation);

    /**
     * Specify the key and value serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withSerializers(Class<? extends Annotation> bindingAnnotationClass);


    /**
     * Specify the key serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withKeySerializer(Key<? extends Function<? super K, String>> functionKey);

    /**
     * Specify the key serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withKeySerializer(Function<? super K, String> keySerializerFunction);

    /**
     * Use the default (Guice bound) key serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withKeySerializer();

    /**
     * Specify the key serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withKeySerializer(Annotation bindingAnnotation);

    /**
     * Specify the key serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withKeySerializer(Class<? extends Annotation> bindingAnnotationClass);

    /**
     * Specify the value serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withValueSerializer(Key<? extends Function<? super V, byte[]>> serializerKey, Key<? extends Function<byte[], ? extends V>> deserializerKey);

    /**
     * Specify the value serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withValueSerializer(Function<? super V, byte[]> valueSerializerFunction, Function<byte[], ? extends V> valueDeserializerFunction);

    /**
     * Specify the value serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withValueSerializer();

    /**
     * Specify the value serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withValueSerializer(Annotation bindingAnnotation);

    /**
     * Specify the value serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withValueSerializer(Class<? extends Annotation> bindingAnnotationClass);

    /**
     * Specify the value serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withValueSerializer(Annotation serializerBindingAnnotation, Annotation deserializerBindingAnnotation);

    /**
     * Specify the value serialization function.
     */
    GuavaCacheModuleBuilder<K, V> withValueSerializer(Class<? extends Annotation> serializerBindingAnnotationClass, Class<? extends Annotation> deserializerBindingAnnotationClass);

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
    Module build(final Key<? extends CacheLoader<? super K, ? extends V>> key);
}
