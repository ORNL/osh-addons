/***************************** BEGIN LICENSE BLOCK ***************************

The contents of this file are subject to the Mozilla Public License, v. 2.0.
If a copy of the MPL was not distributed with this file, You can obtain one
at http://mozilla.org/MPL/2.0/.

Software distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
for the specific language governing rights and limitations under the License.
 
Copyright (C) 2022 Sensia Software LLC. All Rights Reserved.
 
******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.service.hivemq;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.util.Arrays;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.sensorhub.impl.service.WebSocketUtils;
import org.slf4j.Logger;


/**
 * <p>
 * Simple proxy to forward MQTT over websocket packets to a local MQTT TCP port
 * </p>
 *
 * @author Alex Robin
 * @since Jun 9, 2022
 */
public class WebSocketProxy implements WebSocketListener
{
    final InetSocketAddress mqttHost;
    final Logger log;
    final String userID;
    Session session;
    volatile AsynchronousSocketChannel mqttSocket;
    ByteBuffer socketReadBuffer;
    CompletionHandler<Integer, Void> socketReadHandler;
    CompletionHandler<Integer, Void> socketWriteHandler;

    private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean writeInProgress = new AtomicBoolean(false);
    
    
    WebSocketProxy(InetSocketAddress mqttHost, String userID, Logger logger)
    {
        this.mqttHost = mqttHost;
        this.userID = userID;
        this.log = logger;
    }


    static boolean isExpectedDisconnect(Throwable error)
    {
        Throwable current = error;
        for (int depth = 0; current != null && depth < 16; depth++)
        {
            String className = current.getClass().getName();
            if (current instanceof AsynchronousCloseException ||
                current instanceof ClosedChannelException ||
                current instanceof EOFException ||
                "org.eclipse.jetty.io.EofException".equals(className) ||
                "org.eclipse.jetty.websocket.api.CloseException".equals(className))
            {
                return true;
            }
            if (current instanceof IOException || className.contains("WebSocketException"))
            {
                String message = current.getMessage();
                if (message != null)
                {
                    String lower = message.toLowerCase(Locale.ROOT);
                    if (lower.contains("broken pipe") ||
                        lower.contains("connection reset") ||
                        lower.contains("forcibly closed") ||
                        lower.contains("closed channel") ||
                        lower.contains("remoteendpoint unavailable") ||
                        lower.contains("session is closed"))
                    {
                        return true;
                    }
                }
            }
            current = current.getCause();
        }
        return false;
    }


    @Override
    public void onWebSocketConnect(Session session)
    {
        try
        {
            this.session = session;
            WebSocketUtils.logOpen(session, log);
            
            mqttSocket = AsynchronousSocketChannel.open();
            mqttSocket.setOption(StandardSocketOptions.TCP_NODELAY, true);
            try
            {
                mqttSocket.connect(mqttHost).get();
                SocketAddress localAddress = mqttSocket.getLocalAddress();
                if (localAddress instanceof InetSocketAddress)
                    WebSocketProxyAuth.register(((InetSocketAddress)localAddress).getAddress(), userID);
            }
            catch (Exception e)
            {
                WebSocketUtils.closeSession(session, StatusCode.SERVER_ERROR, "Cannot connect to MQTT backend", log);
                return;
            }
            
            // prepare async socket read handler
            socketReadBuffer = ByteBuffer.allocate(1500); // max TCP MTU
            socketReadHandler = new CompletionHandler<>() {
                @Override
                public void completed(Integer readBytes, Void attachment)
                {
                    if (readBytes == null || readBytes < 0)
                        return;

                    if (readBytes > 0)
                    {
                        Session currentSession = WebSocketProxy.this.session;
                        if (currentSession == null || !currentSession.isOpen())
                            return;

                        try
                        {
                            socketReadBuffer.flip();
                            currentSession.getRemote().sendBytes(socketReadBuffer);
                        }
                        catch (Exception e)
                        {
                            if (isExpectedDisconnect(e) || !currentSession.isOpen())
                                log.debug("WebSocket session closed while forwarding MQTT packet: {}", e.toString());
                            else
                                log.error("Error forwarding data to websocket", e);
                            return;
                        }
                    }
                    
                    socketReadBuffer.clear();
                    readFromTcpSocket();
                }

                @Override
                public void failed(Throwable e, Void attachment)
                {
                    if (isExpectedDisconnect(e))
                        log.debug("MQTT TCP socket closed during read: {}", e.toString());
                    else
                        log.error("Error reading data from MQTT TCP socket", e);
                }
            };
            
            // start TCP socket read loop
            readFromTcpSocket();
        }
        catch (IOException e)
        {
            throw new IllegalStateException("Error connecting to MQTT server", e);
        }
    }
    
    
    protected void readFromTcpSocket()
    {
        AsynchronousSocketChannel sock = mqttSocket;
        if (sock != null && sock.isOpen())
        {
            try
            {
                sock.read(socketReadBuffer, null, socketReadHandler);
            }
            catch (RuntimeException e)
            {
                if (isExpectedDisconnect(e) || mqttSocket == null || !sock.isOpen())
                    log.debug("MQTT TCP socket already closed before read: {}", e.toString());
                else
                    log.error("Error starting MQTT TCP socket read", e);
            }
        }
    }
    

    @Override
    public void onWebSocketClose(int statusCode, String reason)
    {
        try
        {
            writeQueue.clear();
            AsynchronousSocketChannel sock = mqttSocket;
            mqttSocket = null;
            if (sock != null)
            {
                sock.close();
            }
            
            WebSocketUtils.logClose(session, statusCode, reason, log);
        }
        catch (IOException e)
        {
            if (isExpectedDisconnect(e))
                log.debug("MQTT TCP socket already closed on disconnect: {}", e.toString());
            else
                log.error("Error closing connection to MQTT server", e);
        }
    }


    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int len) {
        AsynchronousSocketChannel sock = mqttSocket;
        if (sock == null || !sock.isOpen())
            return;

        ByteBuffer buf = ByteBuffer.wrap(Arrays.copyOfRange(payload, offset, offset + len));

        writeQueue.add(buf);
        tryWrite();
    }

    private void tryWrite() {
        if (!writeInProgress.compareAndSet(false, true))
            return;

        AsynchronousSocketChannel sock = mqttSocket;
        if (sock == null || !sock.isOpen()) {
            writeQueue.clear();
            writeInProgress.set(false);
            return;
        }

        ByteBuffer first = writeQueue.poll();
        if (first == null) {
            writeInProgress.set(false);
            return;
        }

        // Wrapper so the handler can mutate the current buffer
        class State {
            ByteBuffer current = first;
        }

        State state = new State();

        try {
            sock.write(state.current, null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer result, Void attachment) {
                    AsynchronousSocketChannel activeSock = mqttSocket;
                    if (activeSock == null || !activeSock.isOpen()) {
                        writeQueue.clear();
                        writeInProgress.set(false);
                        return;
                    }

                    try {
                        // Write the remaining bytes
                        if (state.current.hasRemaining()) {
                            activeSock.write(state.current, null, this);
                            return;
                        }

                        // Get next buffer if finished
                        ByteBuffer next = writeQueue.poll();
                        if (next == null) {
                            writeInProgress.set(false);
                            return;
                        }

                        state.current = next;
                        activeSock.write(state.current, null, this);
                    } catch (RuntimeException e) {
                        writeQueue.clear();
                        writeInProgress.set(false);
                        if (isExpectedDisconnect(e) || mqttSocket == null || !activeSock.isOpen())
                            log.debug("MQTT TCP socket closed while draining write queue: {}", e.toString());
                        else
                            log.error("Error writing to MQTT TCP socket", e);
                    }
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    writeInProgress.set(false);
                    AsynchronousSocketChannel activeSock = mqttSocket;
                    if (isExpectedDisconnect(exc) || activeSock == null || !activeSock.isOpen()) {
                        writeQueue.clear();
                        log.debug("MQTT TCP socket closed during write: {}", exc.toString());
                    } else {
                        log.error("Error writing to MQTT TCP socket", exc);
                    }
                }
            });
        } catch (RuntimeException e) {
            writeQueue.clear();
            writeInProgress.set(false);
            if (isExpectedDisconnect(e) || mqttSocket == null || !sock.isOpen())
                log.debug("MQTT TCP socket closed before write started: {}", e.toString());
            else
                log.error("Error writing to MQTT TCP socket", e);
        }
    }



    @Override
    public void onWebSocketText(String message)
    {
        WebSocketUtils.closeSession(session, StatusCode.BAD_DATA, WebSocketUtils.INPUT_NOT_SUPPORTED, log);
    }


    @Override
    public void onWebSocketError(Throwable cause)
    {
        if (isExpectedDisconnect(cause))
            log.debug("WebSocket client disconnected: {}", cause.toString());
        else
            log.error("Internal websocket proxy error", cause);
    }

}
