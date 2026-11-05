package org.sensorhub.impl.service.hivemq;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedChannelException;
import org.junit.Test;

public class TestWebSocketProxyDisconnect
{
    @Test
    public void detectsExpectedSocketAndWebSocketDisconnects()
    {
        assertTrue(WebSocketProxy.isExpectedDisconnect(new AsynchronousCloseException()));
        assertTrue(WebSocketProxy.isExpectedDisconnect(new ClosedChannelException()));
        assertTrue(WebSocketProxy.isExpectedDisconnect(new EOFException()));
        assertTrue(WebSocketProxy.isExpectedDisconnect(new IOException("Broken pipe")));
        assertTrue(WebSocketProxy.isExpectedDisconnect(new IOException("Connection reset by peer")));
    }

    @Test
    public void preservesUnexpectedErrors()
    {
        assertFalse(WebSocketProxy.isExpectedDisconnect(new IllegalArgumentException("Malformed packet")));
        assertFalse(WebSocketProxy.isExpectedDisconnect(null));
    }
}
