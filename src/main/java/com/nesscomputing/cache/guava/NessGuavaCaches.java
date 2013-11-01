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

import com.google.inject.TypeLiteral;

public class NessGuavaCaches {

    private NessGuavaCaches() { }

    /**
     * Create a cache builder
     *
     * @param cacheName the name associated with the CacheModule to be used
     * @param namespace the namespace to give to NamespacedCache
     */
    public static <K, V> GuavaCacheModuleBuilder<K, V> newModuleBuilder(String cacheName, String namespace, TypeLiteral<K> kType, TypeLiteral<V> vType) {
        return new GuavaCacheModuleBuilderImpl<K, V>(cacheName, namespace, kType, vType);
    }

    /**
     * Create a cache builder
     *
     * @param cacheName the name associated with the CacheModule to be used
     * @param namespace the namespace to give to NamespacedCache
     */
    public static <K, V> GuavaCacheModuleBuilder<K, V> newModuleBuilder(String cacheName, String namespace, Class<K> kType, Class<V> vType) {
        return newModuleBuilder(cacheName, namespace, TypeLiteral.get(kType), TypeLiteral.get(vType));
    }
}
