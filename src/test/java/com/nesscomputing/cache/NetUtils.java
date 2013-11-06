package com.nesscomputing.cache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import com.google.common.base.Throwables;

final class NetUtils
{
    public static int findUnusedPort()
    {
        int port;

        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress(0));
            port = socket.getLocalPort();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return port;
    }
}
