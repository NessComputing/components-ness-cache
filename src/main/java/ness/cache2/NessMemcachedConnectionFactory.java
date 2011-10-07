package ness.cache2;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import com.google.inject.Inject;

import net.spy.memcached.KetamaConnectionFactory;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.protocol.binary.BinaryMemcachedNodeImpl;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.Transcoder;

/**
 * A ConnectionFactory which is Ketama and Binary capable, and uses the custom Ness transcoder.
 */
public class NessMemcachedConnectionFactory extends KetamaConnectionFactory {

    private final CacheConfiguration configuration;

    @Inject
    NessMemcachedConnectionFactory(CacheConfiguration configuration) {
        this.configuration = configuration;
    }

    // Use our custom transcoder

    @SuppressWarnings(value = {"unchecked", "rawtypes"} )
    @Override
    public Transcoder<Object> getDefaultTranscoder() {
        return (Transcoder) new MemcacheByteArrayTranscoder();
    }

    // C+P from BinaryConnectionFactory below

    @Override
    public MemcachedNode createMemcachedNode(SocketAddress sa,
            SocketChannel c, int bufSize) {
        boolean doAuth = false;
        return new BinaryMemcachedNodeImpl(sa, c, bufSize,
            new ArrayBlockingQueue<Operation>(configuration.getReadQueueSize()),
            new ArrayBlockingQueue<Operation>(configuration.getWriteQueueSize()),
            new ArrayBlockingQueue<Operation>(configuration.getIncomingQueueSize()),
            configuration.getOperationQueueBlockTime().getMillis(),
            doAuth,
            getOperationTimeout());
    }

    @Override
    public OperationFactory getOperationFactory() {
        return new BinaryOperationFactory();
    }
}
