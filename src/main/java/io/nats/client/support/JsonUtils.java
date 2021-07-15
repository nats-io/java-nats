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

package io.nats.client.support;

import io.nats.client.api.SequencePair;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.nats.client.support.NatsConstants.COLON;

/**
 * Internal json parsing helpers.
 */
public abstract class JsonUtils {
    public static final String EMPTY_JSON = "{}";

    private static final String STRING_RE  = "\\s*\"(.+?)\"";
    private static final String BOOLEAN_RE =  "\\s*(true|false)";
    private static final String NUMBER_RE =  "\\s*(-?\\d+)";
    private static final String STRING_ARRAY_RE = "\\s*\\[\\s*(\".+?\")\\s*\\]";
    private static final String BEFORE_FIELD_RE = "\"";
    private static final String AFTER_FIELD_RE = "\"\\s*:\\s*";

    private static final String Q = "\"";
    private static final String QCOLONQ = "\":\"";
    private static final String QCOLON = "\":";
    private static final String QCOMMA = "\",";
    private static final String COMMA = ",";
    public static final String OPENQ = "{\"";
    public static final String CLOSE = "}";

    private JsonUtils() {} /* ensures cannot be constructed */

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

    public static Pattern string_pattern(String field) {
        return buildPattern(field, STRING_RE);
    }

    public static Pattern number_pattern(String field) {
        return buildPattern(field, NUMBER_RE);
    }

    public static Pattern boolean_pattern(String field) {
        return buildPattern(field, BOOLEAN_RE);
    }

    public static Pattern string_array_pattern(String field) {
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
    public static String getJsonObject(String objectName, String json) {
        return getJsonObject(objectName, json, EMPTY_JSON);
    }

    public static String getJsonObject(String objectName, String json, String dflt) {
        int[] indexes = getBracketIndexes(objectName, json, '{', '}', 0);
        return indexes == null ? dflt : json.substring(indexes[0], indexes[1] + 1);
    }

    /**
     * Extract a list JSON object strings for list object name. Returns empty list '{}' if not found.
     * Assumes that there are no brackets '{' or '}' in the actual data.
     * @param objectName list object name
     * @param json source json
     * @return object json string
     */
    public static List<String> getObjectList(String objectName, String json) {
        List<String> items = new ArrayList<>();
        int[] indexes = getBracketIndexes(objectName, json, '[', ']', -1);
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

    private static int[] getBracketIndexes(String objectName, String json, char start, char end, int fromIndex) {
        int[] result = new int[] {-1, -1};
        int objStart = json.indexOf(Q + objectName + Q, fromIndex);
        if (objStart != -1) {
            int startIx;
            if (fromIndex != -1) {
                int colonMark = json.indexOf(COLON, objStart) + 1;
                startIx = json.indexOf(start, colonMark);
                for (int x = colonMark; x < startIx; x++) {
                    char c = json.charAt(x);
                    if (!Character.isWhitespace(c)) {
                        return getBracketIndexes(objectName, json, start, end, colonMark);
                    }
                }
            }
            else {
                startIx = json.indexOf(start, objStart);
            }
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
     * @return a string list, empty if no values are found.
     */
    public static List<String> getStringList(String objectName, String json) {
        String flat = json.replaceAll("\r", "").replaceAll("\n", "");
        List<String> list = new ArrayList<>();
        Matcher m = string_array_pattern(objectName).matcher(flat);
        if (m.find()) {
            String arrayString = m.group(1);
            String[] raw = arrayString.split(",");

            for (String s : raw) {
                String cleaned = s.trim().replace("\"", "");
                if (cleaned.length() > 0) {
                    list.add(decode(cleaned));
                }
            }
        }
        return list;
    }

    public static byte[] simpleMessageBody(String name, Number value) {
        return (OPENQ + name + QCOLON + value + CLOSE).getBytes();
    }

    public static byte[] simpleMessageBody(String name, String value) {
        return (OPENQ + name + QCOLONQ + value + Q + CLOSE).getBytes();
    }

    public static StringBuilder beginJson() {
        return new StringBuilder("{");
    }

    public static StringBuilder beginJsonPrefixed(String prefix) {
        return prefix == null ? beginJson()
                : new StringBuilder(prefix).append(NatsConstants.SPACE).append('{');
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
    public static void addField(StringBuilder sb, String fname, String value) {
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
    public static void addField(StringBuilder sb, String fname, boolean value) {
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
            addField(sb, fname, true);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addField(StringBuilder sb, String fname, long value) {
        if (value >= 0) {
            sb.append(Q).append(fname).append(QCOLON).append(value).append(COMMA);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value duration value
     */
    public static void addFieldAsNanos(StringBuilder sb, String fname, Duration value) {
        if (value != null && value != Duration.ZERO) {
            sb.append(Q).append(fname).append(QCOLON).append(value.toNanos()).append(COMMA);
        }
    }

    /**
     * Appends a json object to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value JsonSerializable value
     */
    public static void addField(StringBuilder sb, String fname, JsonSerializable value) {
        if (value != null) {
            sb.append(Q).append(fname).append(QCOLON).append(value.toJson()).append(COMMA);
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param strArray field value
     */
    public static void addStrings(StringBuilder sb, String fname, String[] strArray) {
        if (strArray == null || strArray.length == 0) {
            return;
        }

        addStrings(sb, fname, Arrays.asList(strArray));
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param strings field value
     */
    public static void addStrings(StringBuilder sb, String fname, List<String> strings) {
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
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param jsons field value
     */
    public static void addJsons(StringBuilder sb, String fname, List<? extends JsonSerializable> jsons) {
        if (jsons == null || jsons.size() == 0) {
            return;
        }

        sb.append(Q).append(fname).append("\":[");
        for (int i = 0; i < jsons.size(); i++) {
            JsonSerializable s = jsons.get(i);
            sb.append(s.toJson());
            if (i < jsons.size()-1) {
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
    public static void addField(StringBuilder sb, String fname, ZonedDateTime zonedDateTime) {
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
        return m.find() ? decode(m.group(1)) : dflt;
    }

    public static byte[] readBytes(String json, Pattern pattern) {
        String s = readString(json, pattern, null);
        return s == null ? null : s.getBytes(StandardCharsets.US_ASCII);
    }

    public static byte[] readBase64(String json, Pattern pattern) {
        Matcher m = pattern.matcher(json);
        String b64 = m.find() ? m.group(1) : null;
        return b64 == null ? null : Base64.getDecoder().decode(b64);
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
        return m.find() ? safeParseLong(m.group(1), dflt) : dflt;
    }

    public static void readLong(String json, Pattern pattern, LongConsumer c) {
        Matcher m = pattern.matcher(json);
        if (m.find()) {
            Long l = safeParseLong(m.group(1));
            if (l != null) {
                c.accept(l);
            }
        }
    }

    public static Long safeParseLong(String s) {
        try {
            return Long.parseLong(s);
        }
        catch (Exception e1) {
            try {
                return Long.parseUnsignedLong(s);
            }
            catch (Exception e2) {
                return null;
            }
        }
    }

    public static long safeParseLong(String s, long dflt) {
        Long l = safeParseLong(s);
        return l == null ? dflt : l;
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

    public static String decode(String s) {
        int len = s.length();
        StringBuilder sb = new StringBuilder(len);
        for (int x = 0; x < len; x++) {
            char ch = s.charAt(x);
            if (ch == '\\') {
                char nextChar = (x == len - 1) ? '\\' : s.charAt(x + 1);
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
                    // Hex Unicode: u????
                    case 'u':
                        if (x >= len - 5) {
                            ch = 'u';
                            break;
                        }
                        int code = Integer.parseInt(
                                "" + s.charAt(x + 2) + s.charAt(x + 3) + s.charAt(x + 4) + s.charAt(x + 5), 16);
                        sb.append(Character.toChars(code));
                        x += 5;
                        continue;
                }
                x++;
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    public static String normalize(String s) {
        return Character.toString(s.charAt(0)).toUpperCase() + s.substring(1).toLowerCase();
    }

    public static String objectString(String name, Object o) {
        if (o == null) {
            return name + "=null";
        }
        if (o instanceof SequencePair) {
            return o.toString().replace("SequencePair", name);
        }
        return o.toString();
    }

    // ----------------------------------------------------------------------------------------------------
    // PRINT UTILS
    // ----------------------------------------------------------------------------------------------------

    private static final String INDENT = "                                ";
    private static String indent(int level) {
        return level == 0 ? "" : INDENT.substring(0, level * 4);
    }

    public static void printFormatted(Object o) {
        int level = 0;
        boolean indentNext = true;
        String s = o.toString();
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c == '{') {
                System.out.print(c + "\n");
                ++level;
                indentNext = true;
            }
            else if (c == '}') {
                System.out.print("\n" + indent(--level) + c);
            }
            else if (c == ',') {
                System.out.print("\n");
                indentNext = true;
            }
            else {
                if (indentNext) {
                    if (c != ' ') {
                        System.out.print(indent(level) + c);
                        indentNext = false;
                    }
                }
                else {
                    System.out.print(c);
                }
            }
        }

        System.out.println();
    }
}
