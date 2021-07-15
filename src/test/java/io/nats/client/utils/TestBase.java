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

package io.nats.client.utils;

import io.nats.client.*;
import io.nats.client.impl.NatsMessage;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static io.nats.client.support.NatsConstants.DOT;
import static io.nats.examples.ExampleUtils.uniqueEnough;
import static org.junit.jupiter.api.Assertions.*;

public class TestBase {

    public static final String PLAIN         = "plain";
    public static final String HAS_SPACE     = "has space";
    public static final String HAS_PRINTABLE = "has-print!able";
    public static final String HAS_DOT       = "has.dot";
    public static final String HAS_STAR      = "has*star";
    public static final String HAS_GT        = "has>gt";
    public static final String HAS_DOLLAR    = "has$dollar";
    public static final String HAS_LOW       = "has\tlower\rthan\nspace";
    public static final String HAS_127       = "has" + (char)127 + "127";

    public static final long STANDARD_CONNECTION_WAIT_MS = 5000;
    public static final long STANDARD_FLUSH_TIMEOUT_MS = 2000;
    public static final long MEDIUM_FLUSH_TIMEOUT_MS = 5000;
    public static final long LONG_TIMEOUT_MS = 15000;
    public static final long VERY_LONG_TIMEOUT_MS = 20000;

    // ----------------------------------------------------------------------------------------------------
    // runners
    // ----------------------------------------------------------------------------------------------------
    public interface InServerTest {
        void test(Connection nc) throws Exception;
    }

    public static void runInServer(InServerTest inServerTest) throws Exception {
        runInServer(false, false, inServerTest);
    }

    public static void runInServer(Options.Builder builder, InServerTest inServerTest) throws Exception {
        runInServer(false, false, builder, inServerTest);
    }

    public static void runInServer(boolean debug, InServerTest inServerTest) throws Exception {
        runInServer(debug, false, inServerTest);
    }

    public static void runInJsServer(InServerTest inServerTest) throws Exception {
        runInServer(false, true, inServerTest);
    }

    public static void runInJsServer(boolean debug, InServerTest inServerTest) throws Exception {
        runInServer(debug, true, inServerTest);
    }

    public static void runInServer(boolean debug, boolean jetstream, InServerTest inServerTest) throws Exception {
        try (NatsTestServer ts = new NatsTestServer(debug, jetstream);
             Connection nc = standardConnection(ts.getURI()))
        {
            inServerTest.test(nc);
        }
    }

    public static void runInServer(boolean debug, boolean jetstream, Options.Builder builder, InServerTest inServerTest) throws Exception {
        try (NatsTestServer ts = new NatsTestServer(debug, jetstream);
             Connection nc = standardConnection(builder.server(ts.getURI()).build()))
        {
            inServerTest.test(nc);
        }
    }

    public static void runInExternalServer(InServerTest inServerTest) throws Exception {
        runInExternalServer(Options.DEFAULT_URL, inServerTest);
    }

    public static void runInExternalServer(String url, InServerTest inServerTest) throws Exception {
        try (Connection nc = Nats.connect(url)) {
            inServerTest.test(nc);
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // data makers
    // ----------------------------------------------------------------------------------------------------
    public static final String STREAM = "stream";
    public static final String MIRROR = "mirror";
    public static final String SOURCE = "source";
    public static final String SUBJECT = "subject";
    public static final String SUBJECT_STAR = SUBJECT + ".*";
    public static final String SUBJECT_GT = SUBJECT + ".>";
    public static final String QUEUE = "queue";
    public static final String DURABLE = "durable";
    public static final String DELIVER = "deliver";
    public static final String MESSAGE_ID = "mid";
    public static final String DATA = "data";

    public static String stream(int seq) {
        return STREAM + "-" + seq;
    }

    public static String mirror(int seq) {
        return MIRROR + "-" + seq;
    }

    public static String source(int seq) {
        return SOURCE + "-" + seq;
    }

    public static String subject(int seq) {
        return SUBJECT + "-" + seq;
    }

    public static String subjectDot(String field) {
        return SUBJECT + DOT + field;
    }

    public static String queue(int seq) {
        return QUEUE + "-" + seq;
    }

    public static String durable(int seq) {
        return DURABLE + "-" + seq;
    }

    public static String durable(String vary, int seq) {
        return DURABLE + "-" + vary + "-" + seq;
    }

    public static String deliver(int seq) {
        return DELIVER + "-" + seq;
    }

    public static String messageId(int seq) {
        return MESSAGE_ID + "-" + seq;
    }

    public static String data(int seq) {
        return DATA + "-" + seq;
    }

    public static byte[] dataBytes(int seq) {
        return data(seq).getBytes(StandardCharsets.US_ASCII);
    }

    public static NatsMessage getDataMessage(String data) {
        return new NatsMessage(SUBJECT, null, data.getBytes(StandardCharsets.US_ASCII));
    }

    // ----------------------------------------------------------------------------------------------------
    // assertions
    // ----------------------------------------------------------------------------------------------------
    public static void assertConnected(Connection conn) {
        assertSame(Connection.Status.CONNECTED, conn.getStatus(),
                () -> expectingMessage(conn, Connection.Status.CONNECTED));
    }

    public static void assertNotConnected(Connection conn) {
        assertNotSame(Connection.Status.CONNECTED, conn.getStatus(),
                () -> "Failed not expecting Connection Status " + Connection.Status.CONNECTED.name());
    }

    public static void assertClosed(Connection conn) {
        assertSame(Connection.Status.CLOSED, conn.getStatus(),
                () -> expectingMessage(conn, Connection.Status.CLOSED));
    }

    public static void assertCanConnect() throws IOException, InterruptedException {
        standardCloseConnection( standardConnection() );
    }

    public static void assertCanConnect(String serverURL) throws IOException, InterruptedException {
        standardCloseConnection( standardConnection(serverURL) );
    }

    public static void assertCanConnect(Options options) throws IOException, InterruptedException {
        standardCloseConnection( standardConnection(options) );
    }

    public static void assertCanConnectAndPubSub() throws IOException, InterruptedException {
        Connection conn = standardConnection();
        assertPubSub(conn);
        standardCloseConnection(conn);
    }

    public static void assertCanConnectAndPubSub(String serverURL) throws IOException, InterruptedException {
        Connection conn = standardConnection(serverURL);
        assertPubSub(conn);
        standardCloseConnection(conn);
    }

    public static void assertCanConnectAndPubSub(Options options) throws IOException, InterruptedException {
        Connection conn = standardConnection(options);
        assertPubSub(conn);
        standardCloseConnection(conn);
    }

    public static void assertByteArraysEqual(byte[] data1, byte[] data2) {
        if (data1 == null) {
            assertNull(data2);
            return;
        }
        assertNotNull(data2);
        assertEquals(data1.length, data2.length);
        for (int x = 0; x < data1.length; x++) {
            assertEquals(data1[x], data2[x]);
        }
    }

    public static void assertPubSub(Connection conn) throws InterruptedException {
        String subject = "sub" + uniqueEnough();
        String data = "data" + uniqueEnough();
        Subscription sub = conn.subscribe(subject);
        conn.publish(subject, data.getBytes());
        Message m = sub.nextMessage(Duration.ofSeconds(2));
        assertNotNull(m);
        assertEquals(data, new String(m.getData()));
    }

    // ----------------------------------------------------------------------------------------------------
    // utils / macro utils
    // ----------------------------------------------------------------------------------------------------
    public static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ignored) { /* ignored */ }
    }

    public static void park(Duration d) {
        try { LockSupport.parkNanos(d.toNanos()); } catch (Exception ignored) { /* ignored */ }
    }

    // ----------------------------------------------------------------------------------------------------
    // flush
    // ----------------------------------------------------------------------------------------------------
    public static void flushConnection(Connection conn) {
        flushConnection(conn, Duration.ofMillis(STANDARD_FLUSH_TIMEOUT_MS));
    }

    public static void flushConnection(Connection conn, long timeoutMillis) {
        flushConnection(conn, Duration.ofMillis(timeoutMillis));
    }

    public static void flushConnection(Connection conn, Duration timeout) {
        try { conn.flush(timeout); } catch (Exception exp) { /* ignored */ }
    }

    public static void flushAndWait(Connection conn, TestHandler handler, long flushTimeoutMillis, long waitForStatusMillis) {
        flushConnection(conn, flushTimeoutMillis);
        handler.waitForStatusChange(waitForStatusMillis, TimeUnit.MILLISECONDS);
    }

    public static void flushAndWaitLong(Connection conn, TestHandler handler) {
        flushAndWait(conn, handler, STANDARD_FLUSH_TIMEOUT_MS, LONG_TIMEOUT_MS);
    }

    // ----------------------------------------------------------------------------------------------------
    // connect or wait for a connection
    // ----------------------------------------------------------------------------------------------------

    public static Connection standardConnection() throws IOException, InterruptedException {
        return standardConnectionWait( Nats.connect() );
    }

    public static Connection standardConnection(String serverURL) throws IOException, InterruptedException {
        return standardConnectionWait( Nats.connect(serverURL) );
    }

    public static Connection standardConnection(Options options) throws IOException, InterruptedException {
        return standardConnectionWait( Nats.connect(options) );
    }

    public static Connection standardConnection(Options options, TestHandler handler) throws IOException, InterruptedException {
        return standardConnectionWait( Nats.connect(options), handler );
    }

    public static Connection standardConnectionWait(Connection conn, TestHandler handler) {
        return standardConnectionWait(conn, handler, STANDARD_CONNECTION_WAIT_MS);
    }

    public static Connection standardConnectionWait(Connection conn, TestHandler handler, long millis) {
        handler.waitForStatusChange(millis, TimeUnit.MILLISECONDS);
        assertConnected(conn);
        return conn;
    }

    public static Connection standardConnectionWait(Connection conn) {
        return connectionWait(conn, STANDARD_CONNECTION_WAIT_MS);
    }

    public static Connection connectionWait(Connection conn, long millis) {
        return waitUntilStatus(conn, millis, Connection.Status.CONNECTED);
    }

    // ----------------------------------------------------------------------------------------------------
    // close
    // ----------------------------------------------------------------------------------------------------
    public static void standardCloseConnection(Connection conn) {
        closeConnection(conn, STANDARD_CONNECTION_WAIT_MS);
    }

    public static void closeConnection(Connection conn, long millis) {
        if (conn != null) {
            close(conn);
            waitUntilStatus(conn, millis, Connection.Status.CLOSED);
            assertClosed(conn);
        }
    }

    public static void close(Connection conn) {
        try { conn.close(); } catch (InterruptedException e) { /* ignored */ }
    }

    // ----------------------------------------------------------------------------------------------------
    // connection waiting
    // ----------------------------------------------------------------------------------------------------
    public static Connection waitUntilStatus(Connection conn, long millis, Connection.Status waitUntilStatus) {
        long times = (millis + 99) / 100;
        for (long x = 0; x < times; x++) {
            sleep(100);
            if (conn.getStatus() == waitUntilStatus) {
                return conn;
            }
        }

        throw new AssertionFailedError(expectingMessage(conn, waitUntilStatus));
    }

    private static String expectingMessage(Connection conn, Connection.Status expecting) {
        return "Failed expecting Connection Status " + expecting.name() + " but was " + conn.getStatus();
    }
}
