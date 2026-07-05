package com.patken.transaction.domain.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares how a {@link com.patken.transaction.domain.exception.DomainException} surfaces
 * over HTTP. The {@code GlobalExceptionHandler} reads this once, reflectively, to build an
 * RFC 7807 {@code ProblemDetail} — so adding a new business error means annotating it, not
 * touching the handler (open/closed).
 *
 * <p>The status is a plain int rather than a Spring {@code HttpStatus} to keep the domain
 * layer free of web-framework types; the annotation lives next to {@code @Terminal} because
 * both are domain-level declarations of intent cross-checked by infrastructure.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ProblemMapping {

    /** HTTP status code this error maps to (e.g. 404, 409). */
    int status();

    /** Short, human-readable RFC 7807 {@code title} — stable per error type, not per instance. */
    String title();
}
