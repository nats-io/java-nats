package io.nats.client.impl;

import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamOptions;
import io.nats.client.Message;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerCreateRequest;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.support.NatsJetStreamConstants;

import java.io.IOException;
import java.time.Duration;

class NatsJetStreamImplBase implements NatsJetStreamConstants {

    protected final NatsConnection conn;
    protected final JetStreamOptions jso;

    // ----------------------------------------------------------------------------------------------------
    // Create / Init
    // ----------------------------------------------------------------------------------------------------
    NatsJetStreamImplBase(NatsConnection connection, JetStreamOptions jsOptions) throws IOException {
        conn = connection;
        jso = JetStreamOptions.builder(jsOptions).build(); // builder handles null
    }

    // ----------------------------------------------------------------------------------------------------
    // Management that is also needed by regular context
    // ----------------------------------------------------------------------------------------------------
    protected ConsumerInfo getConsumerInfo(String streamName, String consumer) throws IOException, JetStreamApiException {
        String subj = String.format(JSAPI_CONSUMER_INFO, streamName, consumer);
        Message resp = makeRequestResponseRequired(subj, null, jso.getRequestTimeout());
        return new ConsumerInfo(resp).throwOnHasError();
    }

    protected ConsumerInfo addOrUpdateConsumerInternal(String streamName, ConsumerConfiguration config) throws IOException, JetStreamApiException {
        String durable = config.getDurable();
        String requestJSON = new ConsumerCreateRequest(streamName, config).toJson();

        String subj;
        if (durable == null) {
            subj = String.format(JSAPI_CONSUMER_CREATE, streamName);
        } else {
            subj = String.format(JSAPI_DURABLE_CREATE, streamName, durable);
        }
        Message resp = makeRequestResponseRequired(subj, requestJSON.getBytes(), conn.getOptions().getConnectionTimeout());
        return new ConsumerInfo(resp).throwOnHasError();
    }

    // ----------------------------------------------------------------------------------------------------
    // Request Utils
    // ----------------------------------------------------------------------------------------------------
    protected Message makeRequestResponseRequired(String subject, byte[] bytes, Duration timeout) throws IOException {
        try {
            return responseRequired(conn.request(prependPrefix(subject), bytes, timeout));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    protected Message makeInternalRequestResponseRequired(String subject, Headers headers, byte[] data, boolean utf8mode, Duration timeout, boolean cancelOn503) throws IOException {
        try {
            return responseRequired(conn.requestInternal(subject, headers, data, utf8mode, timeout, cancelOn503));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    protected Message responseRequired(Message respMessage) throws IOException {
        if (respMessage == null) {
            throw new IOException("Timeout or no response waiting for NATS JetStream server");
        }
        return respMessage;
    }

    protected String prependPrefix(String subject) {
        return jso.getPrefix() + subject;
    }
}
