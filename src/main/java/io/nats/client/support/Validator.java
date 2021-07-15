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

import io.nats.client.api.ConsumerConfiguration;

import java.time.Duration;

import static io.nats.client.support.NatsJetStreamConstants.MAX_PULL_SIZE;

public abstract class Validator {
    private Validator() {} /* ensures cannot be constructed */

    public static String validateSubject(String s, boolean required) {
        return validatePrintable(s, "Subject", required);
    }

    public static String validateReplyTo(String s, boolean required) {
        return validatePrintableExceptWildGt(s, "Reply To", required);
    }

    public static String validateQueueName(String s, boolean required) {
        return validatePrintableExceptWildDotGt(s, "Queue", required);
    }

    public static String validateStreamName(String s, boolean required) {
        return validatePrintableExceptWildDotGt(s, "Stream", required);
    }

    public static String validateDurable(String s, boolean required) {
        return validatePrintableExceptWildDotGt(s, "Durable", required);
    }

    public static String validateDurableRequired(String durable, ConsumerConfiguration cc) {
        if (durable == null) {
            if (cc == null) {
                throw new IllegalArgumentException("Durable is required.");
            }
            return validateDurable(cc.getDurable(), true);
        }
        return validateDurable(durable, true);
    }

    public static String validateJetStreamPrefix(String s) {
        return validatePrintableExceptWildGtDollar(s, "Prefix", false);
    }

    public static void validateDirect(boolean direct, String stream, String durable) {
        if (direct && (nullOrEmpty(stream) || nullOrEmpty(durable))) {
            throw new IllegalArgumentException("Stream and Durable are required for direct mode.");
        }
    }

    interface Check {
        String check(String s, String label);
    }

    public static String _validate(String s, String label, boolean required, Check check) {
        if (required) {
            if (nullOrEmpty(s)) {
                throw new IllegalArgumentException(label + " cannot be null or empty [" + s + "]");
            }
        }
        else if (emptyAsNull(s) == null) {
            return null;
        }

        return check.check(s, label);
    }

    public static String validatePrintable(String s, String label, boolean required) {
        return _validate(s, label, required, (ss, ll) -> {
            if (notPrintable(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range [" + s + "]");
            }
            return s;
        });
    }

    public static String validatePrintableExceptWildDotGt(String s, String label, boolean required) {
        return _validate(s, label, required, (ss, ll) -> {
            if (notPrintableOrHasWildGtDot(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include `*`, `.` or `>` [" + s + "]");
            }
            return s;
        });
    }

    public static String validatePrintableExceptWildGt(String s, String label, boolean required) {
        return _validate(s, label, required, (ss, ll) -> {
            if (notPrintableOrHasWildGt(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include `*`, `>` or `$` [" + s + "]");
            }
            return s;
        });
    }

    public static String validatePrintableExceptWildGtDollar(String s, String label, boolean required) {
        return _validate(s, label, required, (ss, ll) -> {
            if (notPrintableOrHasWildGtDollar(s)) {
                throw new IllegalArgumentException(label + " must be in the printable ASCII range and cannot include `*`, `>` or `$` [" + s + "]");
            }
            return s;
        });
    }

    public static int validatePullBatchSize(int pullBatchSize) {
        if (pullBatchSize < 1 || pullBatchSize > MAX_PULL_SIZE) {
            throw new IllegalArgumentException("Pull Batch Size must be between 1 and " + MAX_PULL_SIZE + " inclusive [" + pullBatchSize + "]");
        }
        return pullBatchSize;
    }

    public static long validateMaxConsumers(long max) {
        return validateGtZeroOrMinus1(max, "Max Consumers");
    }

    public static long validateMaxMessages(long max) {
        return validateGtZeroOrMinus1(max, "Max Messages");
    }

    public static long validateMaxBytes(long max) {
        return validateGtZeroOrMinus1(max, "Max Bytes");
    }

    public static long validateMaxMessageSize(long max) {
        return validateGtZeroOrMinus1(max, "Max message size");
    }

    public static int validateNumberOfReplicas(int replicas) {
        if (replicas < 1 || replicas > 5) {
            throw new IllegalArgumentException("Replicas must be from 1 to 5 inclusive.");
        }
        return replicas;
    }

    public static Duration validateDurationRequired(Duration d) {
        if (d == null || d.isZero() || d.isNegative()) {
            throw new IllegalArgumentException("Duration required and must be greater than 0.");
        }
        return d;
    }

    public static Duration validateDurationNotRequiredGtOrEqZero(Duration d) {
        if (d == null) {
            return Duration.ZERO;
        }
        if (d.isNegative()) {
            throw new IllegalArgumentException("Duration must be greater than or equal to 0.");
        }
        return d;
    }

    public static void validateNotNull(Object o, String fieldName) {
        if (o == null) {
            throw new IllegalArgumentException(fieldName + " cannot be null");
        }
    }

    public static long validateGtZeroOrMinus1(long l, String label) {
        if (zeroOrLtMinus1(l)) {
            throw new IllegalArgumentException(label + " must be greater than zero or -1 for unlimited");
        }
        return l;
    }

    public static long validateNotNegative(long l, String label) {
        if (l < 0) {
            throw new IllegalArgumentException(label + " cannot be negative");
        }
        return l;
    }

    // ----------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------
    public static boolean nullOrEmpty(String s) {
        return s == null || s.length() == 0;
    }

    public static boolean notPrintable(String s) {
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < 33 || c > 126) {
                return true;
            }
        }
        return false;
    }

    public static boolean notPrintableOrHasChars(String s, char[] charsToNotHave) {
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c < 33 || c > 126) {
                return true;
            }
            for (char cx : charsToNotHave) {
                if (c == cx) {
                    return true;
                }
            }
        }
        return false;
    }

    static final char[] WILD_GT = {'*', '>'};
    static final char[] WILD_GT_DOT = {'*', '>', '.'};
    static final char[] WILD_GT_DOLLAR = {'*', '>', '$'};

    private static boolean notPrintableOrHasWildGt(String s) {
        return notPrintableOrHasChars(s, WILD_GT);
    }

    private static boolean notPrintableOrHasWildGtDot(String s) {
        return notPrintableOrHasChars(s, WILD_GT_DOT);
    }

    private static boolean notPrintableOrHasWildGtDollar(String s) {
        return notPrintableOrHasChars(s, WILD_GT_DOLLAR);
    }

    public static String emptyAsNull(String s) {
        return nullOrEmpty(s) ? null : s;
    }

    public static boolean zeroOrLtMinus1(long l) {
        return l == 0 || l < -1;
    }
}
