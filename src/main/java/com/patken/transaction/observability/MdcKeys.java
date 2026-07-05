package com.patken.transaction.observability;

/**
 * The four fields every log line should carry when available (spec §Observability):
 * a correlationId to trace a request across API → Kafka → consumer, plus the
 * transaction identity. Structured logging (JSON) surfaces whatever is in the MDC, so
 * populating these keys is all that's needed for them to appear in the logs.
 */
public final class MdcKeys {

    public static final String CORRELATION_ID = "correlationId";
    public static final String TRANSACTION_ID = "transactionId";
    public static final String BUSINESS_ID = "businessId";
    public static final String STATUS = "status";

    /** The HTTP header a caller can supply to continue an existing trace, echoed back on the response. */
    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    private MdcKeys() {
    }
}
