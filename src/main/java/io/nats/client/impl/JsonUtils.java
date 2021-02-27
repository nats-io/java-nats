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

import io.nats.client.support.NatsConstants;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Internal json parsing helpers.
 */
public abstract class JsonUtils {
    public static final String EMPTY_JSON = "{}";

    private static final String STRING_RE  = "\\s*\"(.+?)\"";
    private static final String BOOLEAN_RE =  "\\s*(true|false)";
    private static final String NUMBER_RE =  "\\s*(\\d+)";
    private static final String STRING_ARRAY_RE = "\\s*\\[\\s*(\".+?\")\\s*\\]";
    private static final String BEFORE_FIELD_RE = "\"";
    private static final String AFTER_FIELD_RE = "\"\\s*:\\s*";

    private static final String Q = "\"";
    private static final String QCOLONQ = "\":\"";
    private static final String QCOLON = "\":";
    private static final String QCOMMA = "\",";
    private static final String COMMA = ",";

    private JsonUtils() {} /* for Jacoco */

    public enum FieldType {
        jsonString(STRING_RE),
        jsonBoolean(BOOLEAN_RE),
        jsonNumber(NUMBER_RE),
        jsonStringArray(STRING_ARRAY_RE);

        final String re;
        FieldType(String re) {
            this.re = re;
        }
    }

    public static Pattern buildCustomPattern(String re) {
        return Pattern.compile(re, Pattern.CASE_INSENSITIVE);
    }

    public static Pattern buildStringPattern(String field) {
        return buildPattern(field, STRING_RE);
    }

    public static Pattern buildNumberPattern(String field) {
        return buildPattern(field, NUMBER_RE);
    }

    public static Pattern buildBooleanPattern(String field) {
        return buildPattern(field, BOOLEAN_RE);
    }

    public static Pattern buildStringArrayPattern(String field) {
        return buildPattern(field, STRING_ARRAY_RE);
    }

    /**
     * Builds a json parsing pattern
     * @param fieldName name of the field
     * @param type type of the field.
     * @return pattern.
     */
    public static Pattern buildPattern(String fieldName, FieldType type) {
        return buildPattern(fieldName, type.re);
    }

    public static Pattern buildPattern(String fieldName, String typeRE) {
        return Pattern.compile(BEFORE_FIELD_RE + fieldName + AFTER_FIELD_RE + typeRE, Pattern.CASE_INSENSITIVE);
    }

    /**
     * Extract a JSON object string by object name. Returns empty object '{}' if not found.
     * @param objectName object name
     * @param json source json
     * @return object json string
     */
    public static String getJSONObject(String objectName, String json) {
        int[] indexes = getBracketIndexes(objectName, json, '{', '}');
        return indexes == null ? EMPTY_JSON : json.substring(indexes[0], indexes[1]+1);
    }

    /**
     * Extract a list JSON object strings for list object name. Returns empty list '{}' if not found.
     * Assumes that there are no brackets '{' or '}' in the actual data.
     * @param objectName list object name
     * @param json source json
     * @return object json string
     */
    public static List<String> getObjectArray(String objectName, String json) {
        List<String> items = new ArrayList<>();
        int[] indexes = getBracketIndexes(objectName, json, '[', ']');
        if (indexes != null) {
            StringBuilder item = new StringBuilder();
            int depth = 0;
            for (int x = indexes[0] + 1; x < indexes[1]; x++) {
                char c = json.charAt(x);
                if (c == '{') {
                    item.append(c);
                    depth++;
                } else if (c == '}') {
                    item.append(c);
                    if (--depth == 0) {
                        items.add(item.toString());
                        item.setLength(0);
                    }
                } else if (depth > 0) {
                    item.append(c);
                }
            }
        }
        return items;
    }

    private static int[] getBracketIndexes(String objectName, String json, char start, char end) {
        int[] result = new int[] {-1, -1};
        int objStart = json.indexOf(Q + objectName + Q);
        if (objStart != -1) {
            int startIx = json.indexOf(start, objStart);
            int depth = 1;
            for (int x = startIx + 1; x < json.length(); x++) {
                char c = json.charAt(x);
                if (c == start) {
                    depth++;
                }
                else if (c == end) {
                    if (--depth == 0) {
                        result[0] = startIx;
                        result[1] = x;
                        return result;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Extract a list strings for list object name. Returns empty array if not found.
     * Assumes that there are no brackets '{' or '}' in the actual data.
     * @param objectName object name
     * @param json source json
     * @return a string array, empty if no values are found.
     */
    public static String[] getStringArray(String objectName, String json) {
        // THIS CODE MAKES SOME ASSUMPTIONS THAT THE JSON IS FORMED IN A CONSISTENT MANNER
        // ..."fieldName": [\n      ],...
        // ..."fieldName": [\n      "value"\n    ],...
        // ..."fieldName": [\n      "value",\n      "value2"\n    ],...
        int ix = json.indexOf("\"" + objectName + "\":");
        if (ix != -1) {
            ix = json.indexOf("\"", ix + objectName.length() + 3);
            if (ix != -1) {
                int endx = json.indexOf("]", ix);
                String[] data = json.substring(ix, endx).split(",");
                for (int x = 0; x < data.length; x++) {
                    data[x] = data[x].trim().replaceAll("\"", "");
                }
                return data;
            }
        }

        return new String[0];
    }

    public static StringBuilder beginJson() {
        return new StringBuilder("{");
    }

    public static StringBuilder beginJsonPrefixed(String prefix) {
        return prefix == null ? beginJson()
                : new StringBuilder(prefix).append(NatsConstants.SP).append('{');
    }

    public static StringBuilder endJson(StringBuilder sb) {
        // remove the trailing ','
        sb.setLength(sb.length()-1);
        sb.append("}");
        return sb;
    }

    public static StringBuilder beginFormattedJson() {
        return new StringBuilder("{\n    ");
    }

    public static String endFormattedJson(StringBuilder sb) {
        sb.setLength(sb.length()-1);
        sb.append("\n}");
        return sb.toString().replaceAll(",", ",\n    ");
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addFld(StringBuilder sb, String fname, String value) {
        if (value != null && value.length() > 0) {
            sb.append(Q).append(fname).append(QCOLONQ).append(value).append(QCOMMA);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addFld(StringBuilder sb, String fname, boolean value) {
        sb.append(Q).append(fname).append(QCOLON).append(value ? "true" : "false").append(COMMA);
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addFldWhenTrue(StringBuilder sb, String fname, Boolean value) {
        if (value != null && value) {
            addFld(sb, fname, true);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */    
    public static void addFld(StringBuilder sb, String fname, long value) {
        if (value >= 0) {
            sb.append(Q).append(fname).append(QCOLON).append(value).append(COMMA);
        }      
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addNanoFld(StringBuilder sb, String fname, Duration value) {
        if (value != null && value != Duration.ZERO) {
            sb.append(Q).append(fname).append(QCOLON).append(value.toNanos()).append(COMMA);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param strArray field value
     */
    public static void addFld(StringBuilder sb, String fname, String[] strArray) {
        if (strArray == null || strArray.length == 0) {
            return;
        }

        addFld(sb, fname, Arrays.asList(strArray));
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param strings field value
     */
    public static void addFld(StringBuilder sb, String fname, List<String> strings) {
        if (strings == null || strings.size() == 0) {
            return;
        }

        sb.append(Q).append(fname).append("\":[");
        for (int i = 0; i < strings.size(); i++) {
            String s = strings.get(i);
            sb.append(Q).append(s).append(Q);
            if (i < strings.size()-1) {
                sb.append(COMMA);
            }
        }
        sb.append("],");
    }

    /**
     * Appends a date/time to a string builder as a rfc 3339 formatted field.
     * @param sb string builder
     * @param fname fieldname
     * @param zonedDateTime field value
     */
    public static void addFld(StringBuilder sb, String fname, ZonedDateTime zonedDateTime) {
        if (zonedDateTime != null) {
            sb.append(Q).append(fname).append(QCOLONQ)
                    .append(DateTimeUtils.toRfc3339(zonedDateTime)).append(QCOMMA);
        }
    }

    public static String readString(String json, Pattern pattern) {
        return readString(json, pattern, null);
    }

    public static String readString(String json, Pattern pattern, String dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? m.group(1) : dflt;
    }

    public static boolean readBoolean(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() && Boolean.parseBoolean(m.group(1));
    }

    public static int readInt(String json, Pattern pattern, int dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : dflt;
    }

    public static void readInt(String json, Pattern pattern, IntConsumer c) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            c.accept(Integer.parseInt(m.group(1)));
        }
    }

    public static long readLong(String json, Pattern pattern, long dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Long.parseLong(m.group(1)) : dflt;
    }

    public static void readLong(String json, Pattern pattern, LongConsumer c) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            c.accept(Long.parseLong(m.group(1)));
        }
    }

    public static ZonedDateTime readDate(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        return m.find() ? DateTimeUtils.parseDateTime(m.group(1)) : null;
    }

    public static Duration readNanos(String json, Pattern pattern, Duration dflt) {
        Matcher m = pattern.matcher(json);
        return m.find() ? Duration.ofNanos(Long.parseLong(m.group(1))) : dflt;
    }

    public static void readNanos(String json, Pattern pattern, Consumer<Duration> c) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            c.accept(Duration.ofNanos(Long.parseLong(m.group(1))));
        }
    }
}
