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

import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPushTests extends JetStreamTestBase {

    @ParameterizedTest
    @NullSource // tests null or no deliver subject
    @ValueSource(strings = {DELIVER}) // tests actual deliver subject
    public void testPushEphemeral(String deliverSubject) throws Exception {
        runInJsServer(nc -> {
            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // publish some messages
            jsPublish(js, SUBJECT, 1, 5);

            // Build our subscription options.
            PushSubscribeOptions options = PushSubscribeOptions.builder().deliverSubject(deliverSubject).build();

            // Subscription 1
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            assertSubscription(sub, STREAM, null, deliverSubject, false);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read what is available
            List<Message> messages1 = readMessagesAck(sub);
            int total = messages1.size();
            validateRedAndTotal(5, messages1.size(), 5, total);

            // read again, nothing should be there
            List<Message> messages0 = readMessagesAck(sub);
            total += messages0.size();
            validateRedAndTotal(0, messages0.size(), 5, total);

            // Subscription 2
            sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read what is available, same messages
            List<Message> messages2 = readMessagesAck(sub);
            total = messages2.size();
            validateRedAndTotal(5, messages2.size(), 5, total);

            // read again, nothing should be there
            messages0 = readMessagesAck(sub);
            total += messages0.size();
            validateRedAndTotal(0, messages0.size(), 5, total);

            assertSameMessages(messages1, messages2);
        });
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {DELIVER})
    public void testPushDurable(String deliverSubject) throws Exception {
        runInJsServer(nc -> {
            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // publish some messages
            jsPublish(js, SUBJECT, 1, 5);

            // use ackWait so I don't have to wait forever before re-subscribing
            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackWait(Duration.ofSeconds(3)).build();

            // Build our subscription options.
            PushSubscribeOptions.Builder builder = PushSubscribeOptions.builder()
                    .durable(DURABLE).configuration(cc);
            if (deliverSubject != null) {
                builder.deliverSubject(deliverSubject);
            }
            PushSubscribeOptions options = builder.build();

            // Subscribe.
            JetStreamSubscription sub = js.subscribe(SUBJECT, options);
            assertSubscription(sub, STREAM, DURABLE, deliverSubject, false);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read what is available
            List<Message> messages = readMessagesAck(sub);
            int total = messages.size();
            validateRedAndTotal(5, messages.size(), 5, total);

            // read again, nothing should be there
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 5, total);

            sub.unsubscribe();
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // re-subscribe
            sub = js.subscribe(SUBJECT, options);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // read again, nothing should be there
            messages = readMessagesAck(sub);
            total += messages.size();
            validateRedAndTotal(0, messages.size(), 5, total);
        });
    }

    @Test
    public void testCantPullOnPushSub() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            JetStreamSubscription sub = js.subscribe(SUBJECT);
            assertSubscription(sub, STREAM, null, null, false);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // this should exception, can't pull on a push sub
            assertThrows(IllegalStateException.class, () -> sub.pull(1));
            assertThrows(IllegalStateException.class, () -> sub.pullNoWait(1));
            // TODO pullExpiresIn
//            assertThrows(IllegalStateException.class, () -> sub.pullExpiresIn(1, Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testAcks() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackWait(Duration.ofMillis(1500)).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            // NAK
            jsPublish(js, SUBJECT, "NAK", 1);

            Message message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            String data = new String(message.getData());
            assertEquals("NAK1", data);
            message.nak();

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("NAK1", data);
            message.ack();

            assertNull(sub.nextMessage(Duration.ofSeconds(1)));

            // TERM
            jsPublish(js, SUBJECT, "TERM", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("TERM1", data);
            message.term();

            assertNull(sub.nextMessage(Duration.ofSeconds(1)));

            // Ack Wait timeout
            jsPublish(js, SUBJECT, "WAIT", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("WAIT1", data);
            sleep(2000);
            message.ack(); // this ack came too late so will be ignored

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("WAIT1", data);

            // In Progress
            jsPublish(js, SUBJECT, "PRO", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("PRO1", data);
            message.inProgress();
            sleep(750);
            message.inProgress();
            sleep(750);
            message.inProgress();
            sleep(750);
            message.inProgress();
            sleep(750);
            message.ack();

            assertNull(sub.nextMessage(Duration.ofSeconds(1)));

            // ACK Sync
            jsPublish(js, SUBJECT, "ACKSYNC", 1);

            message = sub.nextMessage(Duration.ofSeconds(1));
            assertNotNull(message);
            data = new String(message.getData());
            assertEquals("ACKSYNC1", data);
            message.ackSync(Duration.ofSeconds(1));

            assertNull(sub.nextMessage(Duration.ofSeconds(1)));
        });
    }

    @Test
    public void testHeartbeat() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .idleHeartbeat(Duration.ofMillis(250))
                    .build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();

            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            int count = 0;
            long now = System.currentTimeMillis();
            long elapsed = System.currentTimeMillis() - now;
            while (elapsed < 3000) {
                Message m = sub.nextMessage(Duration.ofMillis(100));
                if (m != null && m.isStatusMessage() && m.getStatus().isHeartbeat()) {
                    count++;
                }
                elapsed = System.currentTimeMillis() - now;
            }
            assertTrue(count > 8);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"500,1024", "1,500000"})
    public void testFlowControl(String pendingLimits) throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .flowControl(true)
                    .idleHeartbeat(Duration.ofMillis(250))
                    .build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();

            // This is configured so the subscriber ends up being considered slow
            JetStreamSubscription sub = js.subscribe(SUBJECT, pso);
            nc.flush(Duration.ofSeconds(5));
            String[] split = pendingLimits.split(",");
            sub.setPendingLimits(Integer.parseInt(split[0]), Integer.parseInt(split[1]));

            // publish more message data than the subscriber will handle
            byte[] data = new byte[1024];
            for (int x = 1; x <= 100; x++) {
                Message msg = NatsMessage.builder()
                        .subject(SUBJECT)
                        .data(data)
                        .build();
                js.publish(msg);
            }

            // sleep to let the messages back up
            sleep(1000);

            int count = 0;
            long now = System.currentTimeMillis();
            long elapsed = System.currentTimeMillis() - now;
            while (elapsed < 3000 && count < 1) {
                Message m = sub.nextMessage(Duration.ofMillis(100));
                if (m != null && m.isStatusMessage() && m.getStatus().isFlowControl()) {
                    count++;
                }
                elapsed = System.currentTimeMillis() - now;
            }
            assertTrue(count > 0);
        });
    }
}
