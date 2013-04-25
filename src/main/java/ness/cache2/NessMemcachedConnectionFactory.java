package ness.cache2;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.spy.memcached.FailureMode;
import net.spy.memcached.KetamaConnectionFactory;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.Transcoder;

import com.google.inject.Inject;
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
