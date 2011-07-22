package ness.cache;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;

import net.spy.memcached.FailureMode;
import net.spy.memcached.KetamaConnectionFactory;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.OperationFactory;
import net.spy.memcached.protocol.binary.BinaryMemcachedNodeImpl;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.Transcoder;

/**
 * A ConnectionFactory which is Ketama and Binary capable, and uses the custom Ness transcoder.
 */
public class NessMemcachedConnectionFactory extends KetamaConnectionFactory {

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
            createReadOperationQueue(),
            createWriteOperationQueue(),
            createOperationQueue(),
            getOpQueueMaxBlockTime(),
            doAuth,
            getOperationTimeout());
    }
    
    @Override
    public OperationFactory getOperationFactory() {
        return new BinaryOperationFactory();
    }
    
    @Override
    public FailureMode getFailureMode() {
        return FailureMode.Retry;
    }
}
