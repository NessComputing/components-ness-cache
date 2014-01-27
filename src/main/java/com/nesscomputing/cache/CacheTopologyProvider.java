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
package com.nesscomputing.cache;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import org.apache.commons.lang3.StringUtils;

import com.nesscomputing.logging.Log;
import com.nesscomputing.service.discovery.client.ReadOnlyDiscoveryClient;
import com.nesscomputing.service.discovery.client.ServiceInformation;

/**
 * Use service discovery or configuration to assemble a set of Memcache servers dynamically.
 */
@Singleton
class CacheTopologyProvider {
    private static final Function<ServiceInformation, InetSocketAddress> SERVICE_INFORMATION_TO_INET_SOCKET_ADDRESS = new Function<ServiceInformation, InetSocketAddress>() {
        @Override
        public InetSocketAddress apply(ServiceInformation input) {
            return input == null ? null :
                new InetSocketAddress(input.getProperty(ServiceInformation.PROP_SERVICE_ADDRESS),
                    Integer.valueOf(input.getProperty(ServiceInformation.PROP_SERVICE_PORT)));
        }
    };

    private static final Log LOG = Log.findLog();
    private final ImmutableList<InetSocketAddress> addrs;
    private final ReadOnlyDiscoveryClient discoveryClient;
    private final String cacheName;

    @Inject
    CacheTopologyProvider(final CacheConfiguration config, final ReadOnlyDiscoveryClient discoveryClient, @Named("cacheName") String cacheName) {
        this.discoveryClient = discoveryClient;
        this.cacheName = cacheName;

        List<URI> uris = config.getCacheUri();
        if (uris != null) {
            ImmutableList.Builder<InetSocketAddress> addrBuilder = ImmutableList.builder();
            for (URI uri : uris) {
                if ("memcache".equals(uri.getScheme())) {
                    addrBuilder.add(new InetSocketAddress(uri.getHost(), uri.getPort()));
                } else {
                    LOG.warn("Ignored uri %s due to wrong scheme", uri);
                }
            }
            addrs = addrBuilder.build();
            LOG.info("Using configured caches: %s", addrs);
        } else {
            addrs = null;
            LOG.info("Using dynamically discovered caches.");
        }
    }

    public ImmutableList<InetSocketAddress> get() {
        if (addrs != null) {
            return addrs;
        }

        final Collection<ServiceInformation> serviceInformation;
        if (cacheName == null) {
        	serviceInformation = discoveryClient.findAllServiceInformation("memcached");
        }
        else {
        	serviceInformation  = discoveryClient.findAllServiceInformation("memcached", cacheName);
        }

        //apply strict typing
        final Collection<ServiceInformation> discoverInformation = Collections2.filter(serviceInformation, new Predicate<ServiceInformation>() {
            @Override
            public boolean apply(final ServiceInformation input) {
                if (cacheName == null) {
                    return input.getServiceType() == null;
                }
                else {
                    return StringUtils.equals(cacheName, input.getServiceType());
                }
            }
        });

        final List<InetSocketAddress> results = Lists.newArrayList(Collections2.transform(discoverInformation, SERVICE_INFORMATION_TO_INET_SOCKET_ADDRESS));
		Collections.sort(results, InetSocketAddressComparator.DEFAULT);
		return ImmutableList.copyOf(results);
    }
}
