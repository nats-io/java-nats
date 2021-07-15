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
import io.nats.client.support.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamManagementTests extends JetStreamTestBase {

    @Test
    public void testStreamCreate() throws Exception {
        runInJsServer(nc -> {
            long now = ZonedDateTime.now().toEpochSecond();

            JetStreamManagement jsm = nc.jetStreamManagement();

            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(STREAM)
                    .storageType(StorageType.Memory)
                    .subjects(subject(0), subject(1))
                    .build();

            StreamInfo si = jsm.addStream(sc);
            assertTrue(now <= si.getCreateTime().toEpochSecond());

            assertNotNull(si.getConfiguration());
            sc = si.getConfiguration();
            assertEquals(STREAM, sc.getName());

            assertEquals(2, sc.getSubjects().size());
            assertEquals(subject(0), sc.getSubjects().get(0));
            assertEquals(subject(1), sc.getSubjects().get(1));

            assertEquals(RetentionPolicy.Limits, sc.getRetentionPolicy());
            assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(StorageType.Memory, sc.getStorageType());

            assertNotNull(si.getStreamState());
            assertEquals(-1, sc.getMaxConsumers());
            assertEquals(-1, sc.getMaxMsgs());
            assertEquals(-1, sc.getMaxBytes());
            assertEquals(-1, sc.getMaxMsgSize());
            assertEquals(1, sc.getReplicas());

            assertEquals(Duration.ZERO, sc.getMaxAge());
            assertEquals(Duration.ofSeconds(120), sc.getDuplicateWindow());
            assertFalse(sc.getNoAck());
            assertNull(sc.getTemplateOwner());

            StreamState ss = si.getStreamState();
            assertEquals(0, ss.getMsgCount());
            assertEquals(0, ss.getByteCount());
            assertEquals(0, ss.getFirstSequence());
            assertEquals(0, ss.getLastSequence());
            assertEquals(0, ss.getConsumerCount());
        });
    }

    @Test
    public void testStreamCreateWithNoSubject() throws Exception {
        runInJsServer(nc -> {
            long now = ZonedDateTime.now().toEpochSecond();

            JetStreamManagement jsm = nc.jetStreamManagement();

            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(STREAM)
                    .storageType(StorageType.Memory)
                    .build();

            StreamInfo si = jsm.addStream(sc);
            assertTrue(now <= si.getCreateTime().toEpochSecond());

            sc = si.getConfiguration();
            assertEquals(STREAM, sc.getName());

            assertEquals(1, sc.getSubjects().size());
            assertEquals(STREAM, sc.getSubjects().get(0));

            assertEquals(RetentionPolicy.Limits, sc.getRetentionPolicy());
            assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(StorageType.Memory, sc.getStorageType());

            assertNotNull(si.getConfiguration());
            assertNotNull(si.getStreamState());
            assertEquals(-1, sc.getMaxConsumers());
            assertEquals(-1, sc.getMaxMsgs());
            assertEquals(-1, sc.getMaxBytes());
            assertEquals(-1, sc.getMaxMsgSize());
            assertEquals(1, sc.getReplicas());

            assertEquals(Duration.ZERO, sc.getMaxAge());
            assertEquals(Duration.ofSeconds(120), sc.getDuplicateWindow());
            assertFalse(sc.getNoAck());
            assertNull(sc.getTemplateOwner());

            StreamState ss = si.getStreamState();
            assertEquals(0, ss.getMsgCount());
            assertEquals(0, ss.getByteCount());
            assertEquals(0, ss.getFirstSequence());
            assertEquals(0, ss.getLastSequence());
            assertEquals(0, ss.getConsumerCount());
        });
    }

    @Test
    public void testUpdateStream() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamInfo si = addTestStream(jsm);
            StreamConfiguration sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(STREAM, sc.getName());
            assertNotNull(sc.getSubjects());
            assertEquals(2, sc.getSubjects().size());
            assertEquals(subject(0), sc.getSubjects().get(0));
            assertEquals(subject(1), sc.getSubjects().get(1));
            assertEquals(-1, sc.getMaxBytes());
            assertEquals(-1, sc.getMaxMsgSize());
            assertEquals(Duration.ZERO, sc.getMaxAge());
            assertEquals(StorageType.Memory, sc.getStorageType());
            assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(1, sc.getReplicas());
            assertFalse(sc.getNoAck());
            assertEquals(Duration.ofMinutes(2), sc.getDuplicateWindow());
            assertNull(sc.getTemplateOwner());

            sc = StreamConfiguration.builder()
                    .name(STREAM)
                    .storageType(StorageType.Memory)
                    .subjects(subject(0), subject(1), subject(2))
                    .maxBytes(43)
                    .maxMsgSize(44)
                    .maxAge(Duration.ofDays(100))
                    .discardPolicy(DiscardPolicy.New)
                    .noAck(true)
                    .duplicateWindow(Duration.ofMinutes(3))
                    .build();
            si = jsm.updateStream(sc);
            assertNotNull(si);

            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(STREAM, sc.getName());
            assertNotNull(sc.getSubjects());
            assertEquals(3, sc.getSubjects().size());
            assertEquals(subject(0), sc.getSubjects().get(0));
            assertEquals(subject(1), sc.getSubjects().get(1));
            assertEquals(subject(2), sc.getSubjects().get(2));
            assertEquals(43, sc.getMaxBytes());
            assertEquals(44, sc.getMaxMsgSize());
            assertEquals(Duration.ofDays(100), sc.getMaxAge());
            assertEquals(StorageType.Memory, sc.getStorageType());
            assertEquals(DiscardPolicy.New, sc.getDiscardPolicy());
            assertEquals(1, sc.getReplicas());
            assertTrue(sc.getNoAck());
            assertEquals(Duration.ofMinutes(3), sc.getDuplicateWindow());
            assertNull(sc.getTemplateOwner());
        });
    }

    @Test
    public void testAddUpdateStreamInvalids() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            StreamConfiguration scNoName = StreamConfiguration.builder().build();
            assertThrows(IllegalArgumentException.class, () -> jsm.addStream(null));
            assertThrows(IllegalArgumentException.class, () -> jsm.addStream(scNoName));
            assertThrows(IllegalArgumentException.class, () -> jsm.updateStream(null));
            assertThrows(IllegalArgumentException.class, () -> jsm.updateStream(scNoName));

            // cannot update non existent stream
            StreamConfiguration sc = getTestStreamConfiguration();
            // stream not added yet
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(sc));

            // add the stream
            jsm.addStream(sc);

            // cannot change MaxConsumers
            StreamConfiguration scMaxCon = getTestStreamConfigurationBuilder()
                    .maxConsumers(2)
                    .build();
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(scMaxCon));

            // cannot change RetentionPolicy
            StreamConfiguration scReten = getTestStreamConfigurationBuilder()
                    .retentionPolicy(RetentionPolicy.Interest)
                    .build();
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(scReten));
        });
    }

    private static StreamInfo addTestStream(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        StreamInfo si = jsm.addStream(getTestStreamConfiguration());
        assertNotNull(si);
        return si;
    }

    private static StreamConfiguration getTestStreamConfiguration() {
        return getTestStreamConfigurationBuilder().build();
    }

    private static StreamConfiguration.Builder getTestStreamConfigurationBuilder() {
        return StreamConfiguration.builder()
                .name(STREAM)
                .storageType(StorageType.Memory)
                .subjects(subject(0), subject(1));
    }

    @Test
    public void testGetStreamInfo() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            assertThrows(JetStreamApiException.class, () -> jsm.getStreamInfo(STREAM));
            createMemoryStream(jsm, STREAM, SUBJECT);
            StreamInfo si = jsm.getStreamInfo(STREAM);
            assertEquals(STREAM, si.getConfiguration().getName());
        });
    }

    @Test
    public void testDeleteStream() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStreamApiException jsapiEx =
                    assertThrows(JetStreamApiException.class, () -> jsm.deleteStream(STREAM));
            assertEquals(10059, jsapiEx.getApiErrorCode());

            createMemoryStream(jsm, STREAM, SUBJECT);
            assertNotNull(jsm.getStreamInfo(STREAM));
            assertTrue(jsm.deleteStream(STREAM));

            jsapiEx = assertThrows(JetStreamApiException.class, () -> jsm.getStreamInfo(STREAM));
            assertEquals(10059, jsapiEx.getApiErrorCode());

            jsapiEx = assertThrows(JetStreamApiException.class, () -> jsm.deleteStream(STREAM));
            assertEquals(10059, jsapiEx.getApiErrorCode());
        });
    }

    @Test
    public void testPurgeStream() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            assertThrows(JetStreamApiException.class, () -> jsm.purgeStream(STREAM));
            createMemoryStream(jsm, STREAM, SUBJECT);

            StreamInfo si = jsm.getStreamInfo(STREAM);
            assertEquals(0, si.getStreamState().getMsgCount());

            jsPublish(nc, SUBJECT, 1);
            si = jsm.getStreamInfo(STREAM);
            assertEquals(1, si.getStreamState().getMsgCount());

            PurgeResponse pr = jsm.purgeStream(STREAM);
            assertTrue(pr.isSuccess());
            assertEquals(1, pr.getPurgedCount());
        });
    }

    @Test
    public void testAddDeleteConsumer() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createMemoryStream(jsm, STREAM, subject(0), subject(1));

            List<ConsumerInfo> list = jsm.getConsumers(STREAM);
            assertEquals(0, list.size());

            final ConsumerConfiguration cc = ConsumerConfiguration.builder().build();
            IllegalArgumentException iae =
                    assertThrows(IllegalArgumentException.class, () -> jsm.addOrUpdateConsumer(null, cc));
            assertTrue(iae.getMessage().contains("Stream cannot be null or empty"));
            iae = assertThrows(IllegalArgumentException.class, () -> jsm.addOrUpdateConsumer(STREAM, null));
            assertTrue(iae.getMessage().contains("Config cannot be null"));
            iae = assertThrows(IllegalArgumentException.class, () -> jsm.addOrUpdateConsumer(STREAM, cc));
            assertTrue(iae.getMessage().contains("Durable cannot be null"));

            final ConsumerConfiguration cc0 = ConsumerConfiguration.builder()
                    .durable(durable(0))
                    .build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(STREAM, cc0);
            assertEquals(durable(0), ci.getName());
            assertEquals(durable(0), ci.getConsumerConfiguration().getDurable());
            assertNull(ci.getConsumerConfiguration().getDeliverSubject());

            final ConsumerConfiguration cc1 = ConsumerConfiguration.builder()
                    .durable(durable(1))
                    .deliverSubject(deliver(1))
                    .build();
            ci = jsm.addOrUpdateConsumer(STREAM, cc1);
            assertEquals(durable(1), ci.getName());
            assertEquals(durable(1), ci.getConsumerConfiguration().getDurable());
            assertEquals(deliver(1), ci.getConsumerConfiguration().getDeliverSubject());

            List<String> consumers = jsm.getConsumerNames(STREAM);
            assertEquals(2, consumers.size());
            assertTrue(jsm.deleteConsumer(STREAM, cc1.getDurable()));
            consumers = jsm.getConsumerNames(STREAM);
            assertEquals(1, consumers.size());
            assertThrows(JetStreamApiException.class, () -> jsm.deleteConsumer(STREAM, cc1.getDurable()));
        });
    }

    @Test
    public void testInvalidConsumerUpdates() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createMemoryStream(jsm, STREAM, SUBJECT_GT);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(durable(1))
                    .ackPolicy(AckPolicy.Explicit)
                    .deliverSubject(deliver(1))
                    .maxDeliver(3)
                    .filterSubject(SUBJECT_GT)
                    .build();
            assertValidAddOrUpdate(jsm, cc);

            cc = ConsumerConfiguration.builder(cc).deliverSubject(deliver(2)).build();
            assertValidAddOrUpdate(jsm, cc);

            cc = ConsumerConfiguration.builder(cc).deliverPolicy(DeliverPolicy.New).build();
            assertInvalidConsumerUpdate(jsm, cc);

            cc = ConsumerConfiguration.builder(cc).ackWait(Duration.ofSeconds(5)).build();
            assertInvalidConsumerUpdate(jsm, cc);

            cc = ConsumerConfiguration.builder(cc).filterSubject(SUBJECT_STAR).build();
            assertInvalidConsumerUpdate(jsm, cc);

            cc = ConsumerConfiguration.builder(cc).rateLimit(100).build();
            assertInvalidConsumerUpdate(jsm, cc);

            cc = ConsumerConfiguration.builder(cc).maxAckPending(100).build();
            assertInvalidConsumerUpdate(jsm, cc);

            cc = ConsumerConfiguration.builder(cc).idleHeartbeat(Duration.ofMillis(111)).build();
            assertInvalidConsumerUpdate(jsm, cc);

            cc = ConsumerConfiguration.builder(cc).flowControl(true).build();
            assertInvalidConsumerUpdate(jsm, cc);

            cc = ConsumerConfiguration.builder(cc).maxDeliver(4).build();
            assertInvalidConsumerUpdate(jsm, cc);
        });
    }

    private void assertInvalidConsumerUpdate(JetStreamManagement jsm, ConsumerConfiguration cc) {
        JetStreamApiException e = assertThrows(JetStreamApiException.class, () -> jsm.addOrUpdateConsumer(STREAM, cc));
        // 10013 consumer name already in use
        // 10105 consumer already exists and is still active
        assertTrue(e.getApiErrorCode() == 10013 || e.getApiErrorCode() == 10105);
    }

    private void assertValidAddOrUpdate(JetStreamManagement jsm, ConsumerConfiguration cc) throws IOException, JetStreamApiException {
        ConsumerInfo ci = jsm.addOrUpdateConsumer(STREAM, cc);
        ConsumerConfiguration cicc = ci.getConsumerConfiguration();
        assertEquals(cc.getDurable(), ci.getName());
        assertEquals(cc.getDurable(), cicc.getDurable());
        assertEquals(cc.getDeliverSubject(), cicc.getDeliverSubject());
        assertEquals(cc.getMaxDeliver(), cicc.getMaxDeliver());
        assertEquals(cc.getDeliverPolicy(), cicc.getDeliverPolicy());

        List<String> consumers = jsm.getConsumerNames(STREAM);
        assertEquals(1, consumers.size());
        assertEquals(cc.getDurable(), consumers.get(0));
    }

    @Test
    public void testCreateConsumersWithFilters() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            createMemoryStream(jsm, STREAM, SUBJECT);

            // plain subject
            ConsumerConfiguration.Builder builder = ConsumerConfiguration.builder().durable(DURABLE);
            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(SUBJECT).build());

            assertThrows(JetStreamApiException.class,
                    () -> jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("not-match")).build()));

            // wildcard subject
            jsm.deleteStream(STREAM);
            createMemoryStream(jsm, STREAM, SUBJECT_STAR);

            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("A")).build());

            assertThrows(JetStreamApiException.class,
                    () -> jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("not-match")).build()));

            // gt subject
            jsm.deleteStream(STREAM);
            createMemoryStream(jsm, STREAM, SUBJECT_GT);

            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("A")).build());

            assertThrows(JetStreamApiException.class,
                    () -> jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("not-match")).build()));
        });
    }

    @Test
    public void testGetConsumerInfo() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createMemoryStream(jsm, STREAM, SUBJECT);
            assertThrows(JetStreamApiException.class, () -> jsm.getConsumerInfo(STREAM, DURABLE));
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(STREAM, cc);
            assertEquals(STREAM, ci.getStreamName());
            assertEquals(DURABLE, ci.getName());
            ci = jsm.getConsumerInfo(STREAM, DURABLE);
            assertEquals(STREAM, ci.getStreamName());
            assertEquals(DURABLE, ci.getName());
            assertThrows(JetStreamApiException.class, () -> jsm.getConsumerInfo(STREAM, durable(999)));
        });
    }

    @Test
    public void testGetConsumers() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createMemoryStream(jsm, STREAM, subject(0), subject(1));

            addConsumers(jsm, STREAM, 600, "A", null); // getConsumers pages at 256

            List<ConsumerInfo> list = jsm.getConsumers(STREAM);
            assertEquals(600, list.size());

            addConsumers(jsm, STREAM, 500, "B", null); // getConsumerNames pages at 1024
            List<String> names = jsm.getConsumerNames(STREAM);
            assertEquals(1100, names.size());
        });
    }

    private void addConsumers(JetStreamManagement jsm, String stream, int count, String durableVary, String filterSubject) throws IOException, JetStreamApiException {
        for (int x = 1; x <= count; x++) {
            String dur = durable(durableVary, x);
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(dur)
                    .filterSubject(filterSubject)
                    .build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(stream, cc);
            assertEquals(dur, ci.getName());
            assertEquals(dur, ci.getConsumerConfiguration().getDurable());
            assertNull(ci.getConsumerConfiguration().getDeliverSubject());
        }
    }

    @Test
    public void testGetStreams() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            addStreams(jsm, 600, 0); // getStreams pages at 256

            List<StreamInfo> list = jsm.getStreams();
            assertEquals(600, list.size());

            addStreams(jsm, 500, 600); // getStreamNames pages at 1024

            List<String> names = jsm.getStreamNames();
            assertEquals(1100, names.size());
        });
    }

    private void addStreams(JetStreamManagement jsm, int count, int adj) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            createMemoryStream(jsm, stream(x + adj), subject(x + adj));
        }
    }

    @Test
    public void testGetAndDeleteMessage() throws Exception {
        runInJsServer(nc -> {
            createMemoryStream(nc, STREAM, SUBJECT);
            JetStream js = nc.jetStream();

            Headers h = new Headers();
            h.add("foo", "bar");

            ZonedDateTime beforeCreated = ZonedDateTime.now();
            js.publish(NatsMessage.builder().subject(SUBJECT).headers(h).data(dataBytes(1)).build());
            js.publish(NatsMessage.builder().subject(SUBJECT).build());

            JetStreamManagement jsm = nc.jetStreamManagement();

            MessageInfo mi = jsm.getMessage(STREAM, 1);
            assertNotNull(mi.toString());
            assertEquals(SUBJECT, mi.getSubject());
            assertEquals(data(1), new String(mi.getData()));
            assertEquals(1, mi.getSeq());
            assertTrue(mi.getTime().toEpochSecond() >= beforeCreated.toEpochSecond());
            assertNotNull(mi.getHeaders());
            assertEquals("bar", mi.getHeaders().get("foo").get(0));

            mi = jsm.getMessage(STREAM, 2);
            assertNotNull(mi.toString());
            assertEquals(SUBJECT, mi.getSubject());
            assertNull(mi.getData());
            assertEquals(2, mi.getSeq());
            assertTrue(mi.getTime().toEpochSecond() >= beforeCreated.toEpochSecond());
            assertNull(mi.getHeaders());

            assertTrue(jsm.deleteMessage(STREAM, 1));
            assertThrows(JetStreamApiException.class, () -> jsm.deleteMessage(STREAM, 1));
            assertThrows(JetStreamApiException.class, () -> jsm.getMessage(STREAM, 1));
            assertThrows(JetStreamApiException.class, () -> jsm.getMessage(STREAM, 3));
            assertThrows(JetStreamApiException.class, () -> jsm.deleteMessage(stream(999), 1));
            assertThrows(JetStreamApiException.class, () -> jsm.getMessage(stream(999), 1));
        });
    }

    @Test
    public void testMirrorBasics() throws Exception {
        runInJsServer(nc -> {
            String S1 = stream(1);
            String U1 = subject(1);
            String U2 = subject(2);
            String U3 = subject(3);

            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            Mirror mirror = Mirror.builder().sourceName(S1).build();

            // Create source stream
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(S1)
                    .storageType(StorageType.Memory)
                    .subjects(U1, U2, U3)
                    .build();
            StreamInfo si = jsm.addStream(sc);
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S1, sc.getName());

            // Now create our mirror stream.
            sc = StreamConfiguration.builder()
                    .name(mirror(1))
                    .storageType(StorageType.Memory)
                    .mirror(mirror)
                    .build();
            jsm.addStream(sc);
            assertMirror(jsm, mirror(1), S1, null, null);

            // Send 100 messages.
            jsPublish(js, U2, 100);

            // Check the state
            assertMirror(jsm, mirror(1), S1, 100, null);

            // Purge the source stream.
            jsm.purgeStream(S1);

            jsPublish(js, U2, 50);

            // Create second mirror
            sc = StreamConfiguration.builder()
                    .name(mirror(2))
                    .storageType(StorageType.Memory)
                    .mirror(mirror)
                    .build();
            jsm.addStream(sc);

            // Check the state
            assertMirror(jsm, mirror(2), S1, 50, 101);

            jsPublish(js, U3, 100);

            // third mirror checks start seq
            sc = StreamConfiguration.builder()
                    .name(mirror(3))
                    .storageType(StorageType.Memory)
                    .mirror(Mirror.builder().sourceName(S1).startSeq(150).build())
                    .build();
            jsm.addStream(sc);

            // Check the state
            assertMirror(jsm, mirror(3), S1, 101, 150);

            // third mirror checks start seq
            ZonedDateTime zdt = DateTimeUtils.fromNow(Duration.ofHours(-2));
            sc = StreamConfiguration.builder()
                    .name(mirror(4))
                    .storageType(StorageType.Memory)
                    .mirror(Mirror.builder().sourceName(S1).startTime(zdt).build())
                    .build();
            jsm.addStream(sc);

            // Check the state
            assertMirror(jsm, mirror(4), S1, 150, 101);
        });
    }

    @Test
    public void testMirrorExceptions() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            Mirror mirror = Mirror.builder().sourceName(STREAM).build();

            StreamConfiguration scEx = StreamConfiguration.builder()
                    .name(mirror(99))
                    .subjects(subject(1))
                    .mirror(mirror)
                    .build();
            assertThrows(JetStreamApiException.class, () -> jsm.addStream(scEx));
        });
    }

    @Test
    public void testSourceBasics() throws Exception {
        runInJsServer(nc -> {
            String S1 = stream(1);
            String S2 = stream(2);
            String S3 = stream(3);
            String S4 = stream(4);
            String S5 = stream(5);

            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // Create streams
            StreamInfo si = jsm.addStream(StreamConfiguration.builder()
                    .name(S1).storageType(StorageType.Memory).build());
            StreamConfiguration sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S1, sc.getName());

            si = jsm.addStream(StreamConfiguration.builder()
                    .name(S2).storageType(StorageType.Memory).build());
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S2, sc.getName());

            si = jsm.addStream(StreamConfiguration.builder()
                    .name(S3).storageType(StorageType.Memory).build());
            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(S3, sc.getName());

            // Populate each one.
            jsPublish(js, S1, 10);
            jsPublish(js, S2, 15);
            jsPublish(js, S3, 25);

            sc = StreamConfiguration.builder()
                    .name(source(1))
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(S1).build(),
                            Source.builder().sourceName(S2).build(),
                            Source.builder().sourceName(S3).build())
                    .build();

            jsm.addStream(sc);

            assertSource(jsm, source(1), 50, null);

            sc = StreamConfiguration.builder()
                    .name(source(1))
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(S1).build(),
                            Source.builder().sourceName(S2).build(),
                            Source.builder().sourceName(S4).build())
                    .build();

            jsm.updateStream(sc);

            sc = StreamConfiguration.builder()
                    .name(stream(99))
                    .storageType(StorageType.Memory)
                    .subjects(S4, S5)
                    .build();
            jsm.addStream(sc);

            jsPublish(js, S4, 20);
            jsPublish(js, S5, 20);
            jsPublish(js, S4, 10);

            sc = StreamConfiguration.builder()
                    .name(source(2))
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(stream(99)).startSeq(26).build())
                    .build();
            jsm.addStream(sc);
            assertSource(jsm, source(2), 25, null);

            MessageInfo info = jsm.getMessage(source(2), 1);
            assertStreamSource(info, stream(99), 26);

            sc = StreamConfiguration.builder()
                    .name(source(3))
                    .storageType(StorageType.Memory)
                    .sources(Source.builder().sourceName(stream(99)).startSeq(11).filterSubject(S4).build())
                    .build();
            jsm.addStream(sc);
            assertSource(jsm, source(3), 20, null);

            info = jsm.getMessage(source(3), 1);
            assertStreamSource(info, stream(99), 11);
        });
    }

    @Test
    public void testAuthCreateUpdateStream() throws Exception {

        try (NatsTestServer ts = new NatsTestServer("src/test/resources/js_authorization.conf", false)) {
            Options optionsSrc = new Options.Builder().server(ts.getURI())
                    .userInfo("serviceup".toCharArray(), "uppass".toCharArray()).build();

            try (Connection nc = Nats.connect(optionsSrc)) {
                JetStreamManagement jsm = nc.jetStreamManagement();

                // add streams with both account
                StreamConfiguration sc = StreamConfiguration.builder()
                        .name(STREAM)
                        .storageType(StorageType.Memory)
                        .subjects(subject(1))
                        .build();
                StreamInfo si = jsm.addStream(sc);

                sc = StreamConfiguration.builder(si.getConfiguration())
                        .addSubjects(subject(2))
                        .build();

                jsm.updateStream(sc);
            }
        }
    }
}
