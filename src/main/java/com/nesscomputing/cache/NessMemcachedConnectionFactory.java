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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.inject.Inject;

import net.spy.memcached.FailureMode;
import net.spy.memcached.KetamaConnectionFactory;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.Transcoder;
/**
 * A ConnectionFactory which is Ketama and Binary capable, and uses the custom Ness transcoder.
 */
public class NessMemcachedConnectionFactory extends KetamaConnectionFactory {
    private final CacheConfiguration configuration;

    @Inject
    NessMemcachedConnectionFactory(final CacheConfiguration configuration) {
        this.configuration = configuration;
    }

    // Use our custom transcoder

    @SuppressWarnings(value = {"unchecked", "rawtypes"} )
    @Override
    public Transcoder<Object> getDefaultTranscoder() {
        return (Transcoder) new MemcacheByteArrayTranscoder();
    }

    @Override
    public OperationFactory getOperationFactory()
    {
        return new BinaryOperationFactory();
    }

    @Override
    public final int getOpQueueLen()
    {
        return configuration.getMemcachedIncomingQueueSize();
    }

    @Override
    public final BlockingQueue<Operation> createReadOperationQueue()
    {
        int queueSize = configuration.getMemcachedReadQueueSize();
        if (queueSize <= 0) {
            return new LinkedBlockingQueue<Operation>();
        }
        else {
            return new LinkedBlockingQueue<Operation>(queueSize);
        }
    }

    @Override
    public final BlockingQueue<Operation> createWriteOperationQueue()
    {
        int queueSize = configuration.getMemcachedWriteQueueSize();
        if (queueSize <= 0) {
            return new LinkedBlockingQueue<Operation>();
        }
        else {
            return new LinkedBlockingQueue<Operation>(queueSize);
        }
    }

    @Override
    public final long getOpQueueMaxBlockTime()
    {
        return configuration.getMemcachedOperationQueueBlockTime().getMillis();
    }

    @Override
    public final long getOperationTimeout()
    {
        return configuration.getMemcachedOperationTimeout().getMillis();
    }

    @Override
    public boolean isDaemon()
    {
        return configuration.isMemcachedDaemonThreads();
    }

    @Override
    public FailureMode getFailureMode()
    {
        return configuration.getMemcachedFailureMode();
    }

    @Override
    protected String getName()
    {
        return "NessMemcachedConnectionFactory";
    }
}
