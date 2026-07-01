package com.patken.transaction.domain.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a {@link TransactionStatus} constant as terminal — no outgoing transition is
 * ever valid from it. Read via {@link TransactionStatus#isTerminal()}.
 *
 * <p>{@link TransactionStateMachine} cross-checks this annotation against its own
 * transition table at class-init time, so an annotation left out of sync with the
 * table (a status marked terminal that still has an outgoing transition, or vice
 * versa) fails fast instead of silently drifting.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Terminal {
}
