package com.patken.transaction.api;

import com.patken.transaction.domain.annotation.ProblemMapping;
import com.patken.transaction.domain.exception.DomainException;
import com.patken.transaction.observability.MdcKeys;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.net.URI;
import java.util.List;

/**
 * Turns every exception that reaches the dispatcher into an RFC 7807 {@code problem+json}
 * response (the contract's {@code Problem} schema). Business errors are handled as one
 * family: {@link DomainException} carries its own {@link ProblemMapping}, so a new error
 * type is a matter of annotating it — this class never grows a branch per exception.
 *
 * <p>Framework-level bad requests (unreadable body, failed bean validation, wrong param
 * type) map to 400; anything unmapped falls through to a deliberately opaque 500 that
 * leaks no internals. Every response carries the request's {@code correlationId} so a
 * client-reported failure can be traced straight to its logs (Phase 7).
 *
 * <p>401/403 are intentionally absent: authentication/authorization failures are rejected
 * by the Spring Security filter chain before a request ever reaches here.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(DomainException.class)
    public ProblemDetail handleDomain(DomainException ex, HttpServletRequest request) {
        ProblemMapping mapping = ex.getClass().getAnnotation(ProblemMapping.class);
        // Every concrete DomainException is annotated; fall back to 500 rather than NPE if one isn't.
        HttpStatus status = mapping != null ? HttpStatus.valueOf(mapping.status()) : HttpStatus.INTERNAL_SERVER_ERROR;
        String title = mapping != null ? mapping.title() : "Internal error";
        return problem(status, title, ex.getMessage(), request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleBodyValidation(MethodArgumentNotValidException ex, HttpServletRequest request) {
        List<String> violations = ex.getBindingResult().getFieldErrors().stream()
                .map(GlobalExceptionHandler::formatFieldError)
                .sorted()
                .toList();
        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, "Validation failed",
                "One or more fields are invalid", request);
        problem.setProperty("errors", violations);
        return problem;
    }

    @ExceptionHandler(HandlerMethodValidationException.class)
    public ProblemDetail handleParamValidation(HandlerMethodValidationException ex, HttpServletRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "Validation failed",
                "One or more request parameters are invalid", request);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail handleUnreadableBody(HttpMessageNotReadableException ex, HttpServletRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "Malformed request body",
                "Request body is missing or not valid JSON", request);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail handleTypeMismatch(MethodArgumentTypeMismatchException ex, HttpServletRequest request) {
        String detail = "Parameter '%s' has an invalid value: %s".formatted(ex.getName(), ex.getValue());
        return problem(HttpStatus.BAD_REQUEST, "Invalid parameter", detail, request);
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex, HttpServletRequest request) {
        // Log with the stack trace for ops; return an opaque body so internals never leak to clients.
        log.error("Unhandled exception on {} {}", request.getMethod(), request.getRequestURI(), ex);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error",
                "An unexpected error occurred", request);
    }

    private static ProblemDetail problem(HttpStatusCode status, String title, String detail, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        problem.setInstance(URI.create(request.getRequestURI()));
        String correlationId = MDC.get(MdcKeys.CORRELATION_ID);
        if (correlationId != null) {
            problem.setProperty(MdcKeys.CORRELATION_ID, correlationId);
        }
        return problem;
    }

    private static String formatFieldError(FieldError error) {
        return "%s: %s".formatted(error.getField(), error.getDefaultMessage());
    }
}
