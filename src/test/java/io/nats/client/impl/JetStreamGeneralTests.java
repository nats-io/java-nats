// Copyright 2020 The NATS Authors
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
import io.nats.client.api.*;
import io.nats.client.support.JsPrefixManager;
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamGeneralTests extends JetStreamTestBase {

    @Test
    public void testJetStreamContextCreate() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc); // tries management functions
            nc.jetStreamManagement().getAccountStatistics(); // another management
            nc.jetStream().publish(SUBJECT, dataBytes(1));
        });
    }

    @Test
    public void testJetNotEnabled() throws Exception {
        runInServer(nc -> {
            // get normal context, try to do an operation
            JetStream js = nc.jetStream();
            assertThrows(IOException.class, () -> js.subscribe(SUBJECT));

            // get management context, try to do an operation
            JetStreamManagement jsm = nc.jetStreamManagement();
            assertThrows(IOException.class, jsm::getAccountStatistics);
        });
    }

    @Test
    public void testJetEnabledGoodAccount() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/js_authorization.conf", false, true)) {
            Options options = new Options.Builder().server(ts.getURI())
                    .userInfo("serviceup".toCharArray(), "uppass".toCharArray()).build();
            Connection nc = standardConnection(options);
            nc.jetStreamManagement();
            nc.jetStream();
        }
    }

    @Test
    public void testJetStreamPublishDefaultOptions() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            PublishAck ack = jsPublish(js);
            assertEquals(1, ack.getSeqno());
        });
    }

    @Test
    public void testConnectionClosing() throws Exception {
        runInJsServer(nc -> {
            nc.close();
            assertThrows(IOException.class, nc::jetStream);
            assertThrows(IOException.class, nc::jetStreamManagement);
        });
    }

    @Test
    public void testCreateWithOptionsForCoverage() throws Exception {
        runInJsServer(nc -> {
            JetStreamOptions jso = JetStreamOptions.builder().build();
            nc.jetStream(jso);
        });
    }

    @Test
    public void notJetStream() {
        NatsMessage m = NatsMessage.builder().subject("test").build();
        assertThrows(IllegalStateException.class, m::ack);
        assertThrows(IllegalStateException.class, m::nak);
        assertThrows(IllegalStateException.class, () -> m.ackSync(Duration.ZERO));
        assertThrows(IllegalStateException.class, m::inProgress);
        assertThrows(IllegalStateException.class, m::term);
        assertThrows(IllegalStateException.class, m::metaData);
    }

    @Test
    public void testJetStreamSubscribe() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            createTestStream(jsm);
            jsPublish(js);

            // default ephemeral subscription.
            Subscription s = js.subscribe(SUBJECT);
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            List<String> names = jsm.getConsumerNames(STREAM);
            assertEquals(1, names.size());

            // default subscribe options // ephemeral subscription.
            s = js.subscribe(SUBJECT, PushSubscribeOptions.builder().build());
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            names = jsm.getConsumerNames(STREAM);
            assertEquals(2, names.size());

            // set the stream
            PushSubscribeOptions pso = PushSubscribeOptions.builder().stream(STREAM).durable(DURABLE).build();
            s = js.subscribe(SUBJECT, pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            names = jsm.getConsumerNames(STREAM);
            assertEquals(3, names.size());
        });
    }

    @Test
    public void testJetStreamSubscribeErrors() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();

            // stream not found
            PushSubscribeOptions psoInvalidStream = PushSubscribeOptions.builder().stream(STREAM).build();
            assertThrows(JetStreamApiException.class, () -> js.subscribe(SUBJECT, psoInvalidStream));

            // subject
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(null));
            assertTrue(iae.getMessage().startsWith("Subject"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(HAS_SPACE));
            assertTrue(iae.getMessage().startsWith("Subject"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, (PushSubscribeOptions)null));
            assertTrue(iae.getMessage().startsWith("Subject"));

            // queue
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, HAS_SPACE, null));
            assertTrue(iae.getMessage().startsWith("Queue"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, HAS_SPACE, null, null, false, null));
            assertTrue(iae.getMessage().startsWith("Queue"));

            // dispatcher
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, null, null, false));
            assertTrue(iae.getMessage().startsWith("Dispatcher"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, null, null, false, null));
            assertTrue(iae.getMessage().startsWith("Dispatcher"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, QUEUE, null, null, false, null));
            assertTrue(iae.getMessage().startsWith("Dispatcher"));

            // handler
            Dispatcher dispatcher = nc.createDispatcher();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, dispatcher, null, false));
            assertTrue(iae.getMessage().startsWith("Handler"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, dispatcher, null, false, null));
            assertTrue(iae.getMessage().startsWith("Handler"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, QUEUE, dispatcher, null, false, null));
            assertTrue(iae.getMessage().startsWith("Handler"));

            // valid
            createMemoryStream(nc, STREAM, SUBJECT);
            js.subscribe(SUBJECT);
            js.subscribe(SUBJECT, (PushSubscribeOptions)null);
            js.subscribe(SUBJECT, QUEUE, null);
            js.subscribe(SUBJECT, dispatcher, m -> {}, false);
            js.subscribe(SUBJECT, dispatcher, m -> {}, false, null);
            js.subscribe(SUBJECT, QUEUE, dispatcher, m -> {}, false, null);
        });
    }

    @Test
    public void testNoMatchingStreams() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();
            assertThrows(IllegalStateException.class, () -> js.subscribe(SUBJECT));
        });
    }

    @Test
    public void testFilterSubjectEphemeral() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            String subject = SUBJECT + ".*";
            String subjectA = SUBJECT + ".A";
            String subjectB = SUBJECT + ".B";

            // create the stream.
            createMemoryStream(nc, STREAM, subject);

            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);
            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);

            // subscribe to the wildcard
            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackPolicy(AckPolicy.None).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            JetStreamSubscription sub = js.subscribe(subject, pso);
            nc.flush(Duration.ofSeconds(1));

            Message m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(1, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectB, m.getSubject());
            assertEquals(2, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(3, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectB, m.getSubject());
            assertEquals(4, m.metaData().streamSequence());

            // subscribe to A
            cc = ConsumerConfiguration.builder().filterSubject(subjectA).ackPolicy(AckPolicy.None).build();
            pso = PushSubscribeOptions.builder().configuration(cc).build();
            sub = js.subscribe(subject, pso);
            nc.flush(Duration.ofSeconds(1));

            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(1, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(3, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertNull(m);

            // subscribe to B
            cc = ConsumerConfiguration.builder().filterSubject(subjectB).ackPolicy(AckPolicy.None).build();
            pso = PushSubscribeOptions.builder().configuration(cc).build();
            sub = js.subscribe(subject, pso);
            nc.flush(Duration.ofSeconds(1));

            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectB, m.getSubject());
            assertEquals(2, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectB, m.getSubject());
            assertEquals(4, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertNull(m);
        });
    }

    @Test
    public void testFilterSubjectDurable() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            String subjectWild = SUBJECT + ".*";
            String subjectA = SUBJECT + ".A";
            String subjectB = SUBJECT + ".B";

            // create the stream.
            createMemoryStream(nc, STREAM, subjectWild);

            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);
            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);

            ConsumerConfiguration cc = ConsumerConfiguration.builder().filterSubject(subjectA).ackPolicy(AckPolicy.None).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().durable(DURABLE).configuration(cc).build();

            JetStreamSubscription sub = js.subscribe(subjectWild, pso);
            nc.flush(Duration.ofSeconds(1));

            Message m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(1, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(3, m.metaData().streamSequence());
            sub.unsubscribe();

            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);
            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);

            sub = js.subscribe(subjectWild, pso);
            nc.flush(Duration.ofSeconds(1));

            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(5, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(7, m.metaData().streamSequence());
            sub.unsubscribe();

            ConsumerConfiguration cc1 = ConsumerConfiguration.builder().filterSubject(subjectWild).build();
            PushSubscribeOptions pso1 = PushSubscribeOptions.builder().durable(DURABLE).configuration(cc1).build();
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subjectWild, pso1));

            ConsumerConfiguration cc2 = ConsumerConfiguration.builder().filterSubject(subjectB).build();
            PushSubscribeOptions pso2 = PushSubscribeOptions.builder().durable(DURABLE).configuration(cc2).build();
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subjectWild, pso2));
        });
    }

    @Test
    public void testPrefix() throws Exception {
        String prefix = "tar.api";
        String streamMadeBySrc = "stream-made-by-src";
        String streamMadeByTar = "stream-made-by-tar";
        String subjectMadeBySrc = "sub-made-by.src";
        String subjectMadeByTar = "sub-made-by.tar";

        try (NatsTestServer ts = new NatsTestServer("src/test/resources/js_prefix.conf", false)) {
            Options optionsSrc = new Options.Builder().server(ts.getURI())
                    .userInfo("src".toCharArray(), "spass".toCharArray()).build();

            Options optionsTar = new Options.Builder().server(ts.getURI())
                    .userInfo("tar".toCharArray(), "tpass".toCharArray()).build();

            try (Connection ncSrc = Nats.connect(optionsSrc);
                 Connection ncTar = Nats.connect(optionsTar)
            ) {
                // Setup JetStreamOptions. SOURCE does not need prefix
                JetStreamOptions jsoSrc = JetStreamOptions.builder().build();
                JetStreamOptions jsoTar = JetStreamOptions.builder().prefix(prefix).build();

                // Management api allows us to create streams
                JetStreamManagement jsmSrc = ncSrc.jetStreamManagement(jsoSrc);
                JetStreamManagement jsmTar = ncTar.jetStreamManagement(jsoTar);

                // add streams with both account
                StreamConfiguration scSrc = StreamConfiguration.builder()
                        .name(streamMadeBySrc)
                        .storageType(StorageType.Memory)
                        .subjects(subjectMadeBySrc)
                        .build();
                jsmSrc.addStream(scSrc);

                StreamConfiguration scTar = StreamConfiguration.builder()
                        .name(streamMadeByTar)
                        .storageType(StorageType.Memory)
                        .subjects(subjectMadeByTar)
                        .build();
                jsmTar.addStream(scTar);

                JetStream jsSrc = ncSrc.jetStream(jsoSrc);
                JetStream jsTar = ncTar.jetStream(jsoTar);

                jsSrc.publish(subjectMadeBySrc, "src-src".getBytes());
                jsSrc.publish(subjectMadeByTar, "src-tar".getBytes());
                jsTar.publish(subjectMadeBySrc, "tar-src".getBytes());
                jsTar.publish(subjectMadeByTar, "tar-tar".getBytes());

                // subscribe and read messages
                readPrefixMessages(ncSrc, jsSrc, subjectMadeBySrc, "src");
                readPrefixMessages(ncSrc, jsSrc, subjectMadeByTar, "tar");
                readPrefixMessages(ncTar, jsTar, subjectMadeBySrc, "src");
                readPrefixMessages(ncTar, jsTar, subjectMadeByTar, "tar");
            }
        }
    }

    private void readPrefixMessages(Connection nc, JetStream js, String subject, String dest) throws InterruptedException, IOException, JetStreamApiException, TimeoutException {
        JetStreamSubscription sub = js.subscribe(subject);
        nc.flush(Duration.ofSeconds(1));
        List<Message> msgs = readMessagesAck(sub);
        assertEquals(2, msgs.size());
        assertEquals(subject, msgs.get(0).getSubject());
        assertEquals(subject, msgs.get(1).getSubject());

        assertEquals("src-" + dest, new String(msgs.get(0).getData()));
        assertEquals("tar-" + dest, new String(msgs.get(1).getData()));
    }


    @Test
    public void testPrefixManager() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> JsPrefixManager.addPrefix(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> JsPrefixManager.addPrefix(HAS_GT));
        assertThrows(IllegalArgumentException.class, () -> JsPrefixManager.addPrefix(HAS_STAR));

        JsPrefixManager.addPrefix("foo");
        JsPrefixManager.addPrefix("bar.");

        assertTrue(JsPrefixManager.hasPrefix(NatsJetStreamConstants.JS_PREFIX));
        assertTrue(JsPrefixManager.hasPrefix(NatsJetStreamConstants.JSAPI_PREFIX));
        assertTrue(JsPrefixManager.hasPrefix("foo.blah"));
        assertTrue(JsPrefixManager.hasPrefix("bar.blah"));
        assertFalse(JsPrefixManager.hasPrefix("not"));
    }

    @Test
    public void testJetStreamSubscribeDirectPush() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();

            jsPublish(js, SUBJECT, 1, 1);
            PushSubscribeOptions pso = PushSubscribeOptions.builder()
                    .durable(DURABLE)
                    .build();
            JetStreamSubscription s = js.subscribe(SUBJECT, pso);
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(1), new String(m.getData()));
            m.ack();
            s.unsubscribe();

            jsPublish(js, SUBJECT, 2, 1);
            pso = PushSubscribeOptions.builder()
                    .stream(STREAM)
                    .durable(DURABLE)
                    .direct()
                    .build();
            s = js.subscribe(SUBJECT, pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(2), new String(m.getData()));
            m.ack();
            s.unsubscribe();

            jsPublish(js, SUBJECT, 3, 1);
            pso = PushSubscribeOptions.direct(STREAM, DURABLE);
            s = js.subscribe(SUBJECT, pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(3), new String(m.getData()));
        });
    }

    @Test
    public void testJetStreamSubscribeDirectPull() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();

            jsPublish(js, SUBJECT, 1, 1);

            PullSubscribeOptions pso = PullSubscribeOptions.builder()
                    .durable(DURABLE)
                    .build();
            JetStreamSubscription s = js.subscribe(SUBJECT, pso);
            s.pull(1);
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(1), new String(m.getData()));
            m.ack();
            s.unsubscribe();

            jsPublish(js, SUBJECT, 2, 1);
            pso = PullSubscribeOptions.builder()
                    .stream(STREAM)
                    .durable(DURABLE)
                    .direct()
                    .build();
            s = js.subscribe(SUBJECT, pso);
            s.pull(1);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(2), new String(m.getData()));
            m.ack();
            s.unsubscribe();

            jsPublish(js, SUBJECT, 3, 1);
            pso = PullSubscribeOptions.direct(STREAM, DURABLE);
            s = js.subscribe(SUBJECT, pso);
            s.pull(1);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(3), new String(m.getData()));
        });
    }

    @Test
    public void testJetStreamSubscribeDirectNotCreated() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();
            createTestStream(nc);

            PushSubscribeOptions pushso = PushSubscribeOptions.direct(STREAM, DURABLE);
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, pushso));

            PullSubscribeOptions pullso = PullSubscribeOptions.direct(STREAM, DURABLE);
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, pullso));
        });
    }

    @Test
    public void testGetConsumerInfo() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            JetStreamSubscription sub = js.subscribe(SUBJECT);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            ConsumerInfo ci = sub.getConsumerInfo();
            assertEquals(STREAM, ci.getStreamName());
        });
    }
}