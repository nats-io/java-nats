// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.ConnectionListener.Events;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.nats.client.utils.TestBase.standardCloseConnection;
import static io.nats.client.utils.TestBase.standardConnection;
import static org.junit.jupiter.api.Assertions.*;

public class ErrorListenerTests {

    @Test
    public void testLastError() throws Exception {
        NatsConnection nc = null;
        TestHandler handler = new TestHandler();
        String[] customArgs = {"--user", "stephen", "--pass", "password"};

        try (NatsTestServer ts = new NatsTestServer();
             NatsTestServer ts2 = new NatsTestServer(customArgs, false); //ts2 requires auth
             NatsTestServer ts3 = new NatsTestServer()) {
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    server(ts2.getURI()).
                    server(ts3.getURI()).
                    noRandomize().
                    connectionListener(handler).
                    maxReconnects(-1).
                    build();
            nc = (NatsConnection) Nats.connect(options);
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            assertEquals(ts.getURI(), nc.getConnectedUrl());
            handler.prepForStatusChange(Events.DISCONNECTED);

            ts.close();

            try {
                nc.flush(Duration.ofSeconds(1));
            }
            catch (Exception exp) {
                // this usually fails
            }

            handler.waitForStatusChange(5, TimeUnit.SECONDS);

            handler.prepForStatusChange(Events.RECONNECTED);
            handler.waitForStatusChange(5, TimeUnit.SECONDS);

            assertNotNull(nc.getLastError());
            assertTrue(nc.getLastError().contains("Authorization Violation"));
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            assertEquals(ts3.getURI(), nc.getConnectedUrl());
        } finally {
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testErrorOnNoAuth() throws Exception {
        String[] customArgs = {"--user", "stephen", "--pass", "password"};
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                    server(ts.getURI())
                    .maxReconnects(0)
                    .errorListener(handler)
                    // skip this so we get an error userInfo("stephen", "password").
                    .build();
            assertThrows(AuthenticationException.class, () -> Nats.connect(options));
            assertTrue(handler.getCount() > 0);
            assertEquals(1, handler.getErrorCount("Authorization Violation"));
        }
    }

    @Test
    public void testExceptionOnBadDispatcher() throws Exception {
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    maxReconnects(0).
                    errorListener(handler).
                    build();
            Connection nc = Nats.connect(options);
            try {
                Dispatcher d = nc.createDispatcher((msg) -> {
                    throw new ArithmeticException();
                });
                d.subscribe("subject");
                Future<Message> incoming = nc.request("subject", null);

                Message msg;

                try {
                    msg = incoming.get(200, TimeUnit.MILLISECONDS);
                } catch (TimeoutException te) {
                    msg = null;
                }

                assertNull(msg);
                assertEquals(1, handler.getCount());
            } finally {
                standardCloseConnection(nc);
            }
        }
    }

    @Test
    public void testExceptionInErrorHandler() throws Exception {
        String[] customArgs = {"--user", "stephen", "--pass", "password"};
        BadHandler handler = new BadHandler();
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder()
                    .server(ts.getURI())
                    .maxReconnects(0)
                    .errorListener(handler)
                    // skip this so we get an error userInfo("stephen", "password").
                    .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testExceptionInSlowConsumerHandler() throws Exception {
        BadHandler handler = new BadHandler();
        try (NatsTestServer ts = new NatsTestServer(false);
             NatsConnection nc = (NatsConnection) Nats.connect(new Options.Builder().
                     server(ts.getURI()).
                     errorListener(handler).
                     build())) {

            Subscription sub = nc.subscribe("subject");
            sub.setPendingLimits(1, -1);

            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.publish("subject", null);

            nc.flush(Duration.ofMillis(5000));

            assertEquals(3, sub.getDroppedCount());

            nc.close(); // should force the exception handler through

            assertTrue(nc.getNatsStatistics().getExceptions() > 0);
        }
    }

    @Test
    public void testExceptionInExceptionHandler() throws Exception {
        BadHandler handler = new BadHandler();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    maxReconnects(0).
                    errorListener(handler).
                    build();
            Connection nc = Nats.connect(options);
            try {
                Dispatcher d = nc.createDispatcher((msg) -> {
                    throw new ArithmeticException();
                });
                d.subscribe("subject");
                Future<Message> incoming = nc.request("subject", null);

                Message msg;

                try {
                    msg = incoming.get(200, TimeUnit.MILLISECONDS);
                } catch (TimeoutException te) {
                    msg = null;
                }

                assertNull(msg);
                assertEquals(((NatsConnection) nc).getNatsStatistics().getExceptions(), 2); // 1 for the dispatcher, 1 for the handlers
            } finally {
                standardCloseConnection(nc);
            }
        }
    }

    @Test
    public void testDiscardedMessageFastProducer() throws Exception {
        int maxMessages = 10;
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    maxMessagesInOutgoingQueue(maxMessages).
                    discardMessagesWhenOutgoingQueueFull().
                    errorListener(handler).
                    pingInterval(Duration.ofSeconds(100)). // make this long so we don't ping during test
                    build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);

            try {
                nc.flush(Duration.ofSeconds(2));

                nc.getWriter().stop().get(2, TimeUnit.SECONDS);
                for (int i = 0; i < maxMessages + 1; i++) {
                    nc.publish("subject" + i, ("message" + i).getBytes());
                }
                nc.getWriter().start(nc.getDataPortFuture());

                nc.flush(Duration.ofSeconds(2));
            } finally {
                standardCloseConnection(nc);
            }
        }

        List<Message> discardedMessages = handler.getDiscardedMessages();
        assertEquals(1, discardedMessages.size());
        assertEquals("subject10", discardedMessages.get(0).getSubject());
        assertEquals("message10", new String(discardedMessages.get(0).getData()));
    }

    @Test
    public void testDiscardedMessageServerClosed() throws Exception {
        int maxMessages = 10;
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    maxMessagesInOutgoingQueue(maxMessages).
                    discardMessagesWhenOutgoingQueueFull().
                    pingInterval(Duration.ofSeconds(100)). // make this long so we don't ping during test
                    connectionListener(handler).
                    errorListener(handler).
                    build();
            Connection nc = standardConnection(options);

            try {
                nc.flush(Duration.ofSeconds(1)); // Get the sub to the server

                handler.prepForStatusChange(Events.DISCONNECTED);
                ts.close();
                handler.waitForStatusChange(2, TimeUnit.SECONDS); // make sure the connection is down

                for (int i = 0; i < maxMessages + 1; i++) {
                    nc.publish("subject" + i, ("message" + i).getBytes());
                }
            } finally {
                standardCloseConnection(nc);
            }
        }

        List<Message> discardedMessages = handler.getDiscardedMessages();
        assertTrue(discardedMessages.size() >= 1, "At least one message discarded");
        assertTrue(discardedMessages.get(0).getSubject().startsWith("subject"), "Message subject");
        assertTrue(new String(discardedMessages.get(0).getData()).startsWith("message"), "Message data");
    }

    @Test
    public void testMessageDiscardedDefaultImplementation() {
        ErrorListener testErrorListener = new ErrorListener() {
            @Override
            public void errorOccurred(Connection conn, String error) {}

            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {}

            @Override
            public void slowConsumerDetected(Connection conn, Consumer consumer) {}
        };

        testErrorListener.messageDiscarded(null, null); // COVERAGE
    }
}
