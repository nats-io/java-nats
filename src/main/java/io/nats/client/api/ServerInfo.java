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

package io.nats.client.api;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.*;

public class ServerInfo {

    private String serverId;
    private String serverName;
    private String version;
    private String go;
    private String host;
    private int port;
    private boolean headersSupported;
    private boolean authRequired;
    private boolean tlsRequired;
    private long maxPayload;
    private String[] connectURLs;
    private String rawInfoJson;
    private int protocolVersion;
    private byte[] nonce;
    private boolean lameDuckMode;
    private boolean jetStream;
    private int clientId;
    private String clientIp;
    private String cluster;

    public ServerInfo(String json) {
        this.rawInfoJson = json;
        parseInfo(json);
    }

    public boolean isLameDuckMode() {
        return lameDuckMode;
    }

    public String getServerId() {
        return this.serverId;
    }

    public String getServerName() {
        return serverName;
    }

    public String getVersion() {
        return this.version;
    }

    public String getGoVersion() {
        return this.go;
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public int getProtocolVersion() {
        return this.protocolVersion;
    }

    public boolean isHeadersSupported() { return this.headersSupported; }

    public boolean isAuthRequired() {
        return this.authRequired;
    }

    public boolean isTLSRequired() {
        return this.tlsRequired;
    }

    public long getMaxPayload() {
        return this.maxPayload;
    }

    public String[] getConnectURLs() {
        return this.connectURLs;
    }

    public byte[] getNonce() {
        return this.nonce;
    }

    public boolean isJetStreamAvailable() {
        return this.jetStream;
    }

    public int getClientId() {
        return clientId;
    }

    public String getClientIp() {
        return clientIp;
    }

    public String getCluster() {
        return cluster;
    }

    // If parsing succeeds this is the JSON, if not this may be the full protocol line
    public String getRawJson() {
        return rawInfoJson;
    }

    private static final Pattern lameDuckModeRE = boolean_pattern(LAME_DUCK_MODE);
    private static final Pattern jetStreamRE = boolean_pattern(JETSTREAM);
    private static final Pattern serverIdRE = string_pattern(SERVER_ID);
    private static final Pattern serverNameRE = string_pattern(SERVER_NAME);
    private static final Pattern versionRE = string_pattern(VERSION);
    private static final Pattern goRE = string_pattern(GO);
    private static final Pattern hostRE = string_pattern(HOST);
    private static final Pattern nonceRE = string_pattern(NONCE);
    private static final Pattern headersRE = boolean_pattern(HEADERS);
    private static final Pattern authRE = boolean_pattern(AUTH);
    private static final Pattern tlsRE = boolean_pattern(TLS);
    private static final Pattern portRE = number_pattern(PORT);
    private static final Pattern maxRE = number_pattern(MAX_PAYLOAD);
    private static final Pattern protoRE = number_pattern(PROTOCOL_VERSION);
    private static final Pattern connectRE = string_array_pattern(CONNECT_URLS);
    private static final Pattern clientIdRE = number_pattern(CLIENT_ID);
    private static final Pattern clientIpRE = string_pattern(CLIENT_IP);
    private static final Pattern clusterRE = string_pattern(CLUSTER);
    private static final Pattern infoObject = custom_pattern("\\{(.+?)\\}");

    void parseInfo(String jsonString) {

        Matcher m = infoObject.matcher(jsonString);
        if (m.find()) {
            jsonString = m.group(0);
            this.rawInfoJson = jsonString;
        } else {
            jsonString = "";
        }

        if (jsonString.length() < 2) {
            throw new IllegalArgumentException("Server info requires at least {}.");
        } else if (jsonString.charAt(0) != '{' || jsonString.charAt(jsonString.length()-1) != '}') {
            throw new IllegalArgumentException("Server info should be JSON wrapped with { and }.");
        }

        m = serverIdRE.matcher(jsonString);
        if (m.find()) {
            this.serverId = unescapeString(m.group(1));
        }

        m = serverNameRE.matcher(jsonString);
        if (m.find()) {
            this.serverName = unescapeString(m.group(1));
        }

        m = versionRE.matcher(jsonString);
        if (m.find()) {
            this.version = unescapeString(m.group(1));
        }
        
        m = goRE.matcher(jsonString);
        if (m.find()) {
            this.go = unescapeString(m.group(1));
        }
        
        m = hostRE.matcher(jsonString);
        if (m.find()) {
            this.host = unescapeString(m.group(1));
        }

        m = headersRE.matcher(jsonString);
        if (m.find()) {
            this.headersSupported = Boolean.parseBoolean(m.group(1));
        }

        m = authRE.matcher(jsonString);
        if (m.find()) {
            this.authRequired = Boolean.parseBoolean(m.group(1));
        }

        m = nonceRE.matcher(jsonString);
        if (m.find()) {
            String encodedNonce = m.group(1);
            this.nonce = encodedNonce.getBytes(StandardCharsets.US_ASCII);
        }
        
        m = tlsRE.matcher(jsonString);
        if (m.find()) {
            this.tlsRequired = Boolean.parseBoolean(m.group(1));
        }

        m = lameDuckModeRE.matcher(jsonString);
        if (m.find()) {
            this.lameDuckMode = Boolean.parseBoolean(m.group(1));
        }

        m = jetStreamRE.matcher(jsonString);
        if (m.find()) {
            this.jetStream = Boolean.parseBoolean(m.group(1));
        }

        m = portRE.matcher(jsonString);
        if (m.find()) {
            this.port = Integer.parseInt(m.group(1));
        }
        
        m = protoRE.matcher(jsonString);
        if (m.find()) {
            this.protocolVersion = Integer.parseInt(m.group(1));
        }
        
        m = maxRE.matcher(jsonString);
        if (m.find()) {
            this.maxPayload = Long.parseLong(m.group(1));
        }

        m = clientIdRE.matcher(jsonString);
        if (m.find()) {
            this.clientId = Integer.parseInt(m.group(1));
        }

        m = clientIpRE.matcher(jsonString);
        if (m.find()) {
            this.clientIp = unescapeString(m.group(1));
        }

        m = clusterRE.matcher(jsonString);
        if (m.find()) {
            this.cluster = unescapeString(m.group(1));
        }

        m = connectRE.matcher(jsonString);
        if (m.find()) {
            String arrayString = m.group(1);
            String[] raw = arrayString.split(",");
            List<String> urls = new ArrayList<>();

            for (String s : raw) {
                String cleaned = s.trim().replace("\"", "");
                if (cleaned.length() > 0) {
                    urls.add(cleaned);
                }
            }

            this.connectURLs = urls.toArray(new String[0]);
        }
    }

    // See https://gist.github.com/uklimaschewski/6741769, no license required
    // Removed octal support
    String unescapeString(String st) {

        StringBuilder sb = new StringBuilder(st.length());

        for (int i = 0; i < st.length(); i++) {
            char ch = st.charAt(i);
            if (ch == '\\') {
                char nextChar = (i == st.length() - 1) ? '\\' : st.charAt(i + 1);
                switch (nextChar) {
                case '\\':
                    ch = '\\';
                    break;
                case 'b':
                    ch = '\b';
                    break;
                case 'f':
                    ch = '\f';
                    break;
                case 'n':
                    ch = '\n';
                    break;
                case 'r':
                    ch = '\r';
                    break;
                case 't':
                    ch = '\t';
                    break;
                /*case '\"':
                    ch = '\"';
                    break;
                case '\'':
                    ch = '\'';
                    break;*/
                // Hex Unicode: u????
                case 'u':
                    if (i >= st.length() - 5) {
                        ch = 'u';
                        break;
                    }
                    int code = Integer.parseInt(
                            "" + st.charAt(i + 2) + st.charAt(i + 3) + st.charAt(i + 4) + st.charAt(i + 5), 16);
                    sb.append(Character.toChars(code));
                    i += 5;
                    continue;
                }
                i++;
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "NatsServerInfo{" +
                "serverId='" + serverId + '\'' +
                ", serverName='" + serverName + '\'' +
                ", version='" + version + '\'' +
                ", go='" + go + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", headersSupported=" + headersSupported +
                ", authRequired=" + authRequired +
                ", tlsRequired=" + tlsRequired +
                ", maxPayload=" + maxPayload +
                ", connectURLs=" + Arrays.toString(connectURLs) +
                ", rawInfoJson='" + rawInfoJson + '\'' +
                ", protocolVersion=" + protocolVersion +
                ", nonce=" + Arrays.toString(nonce) +
                ", lameDuckMode=" + lameDuckMode +
                ", jetStream=" + jetStream +
                ", clientId=" + clientId +
                ", clientIp='" + clientIp + '\'' +
                ", cluster='" + cluster + '\'' +
                '}';
    }
}