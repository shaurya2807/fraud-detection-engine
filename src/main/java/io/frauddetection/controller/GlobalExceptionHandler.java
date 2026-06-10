package io.frauddetection.controller;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    /**
     * Handles explicit 4xx/5xx throws from service and consumer layers.
     * The reason string (if set) is surfaced as the detail; status comes
     * directly from the exception.
     */
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<ProblemDetail> handleResponseStatus(
            ResponseStatusException ex, HttpServletRequest req) {

        String detail = ex.getReason() != null ? ex.getReason() : ex.getStatusCode().toString();
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(ex.getStatusCode(), detail);
        pd.setInstance(URI.create(req.getRequestURI()));
        return ResponseEntity.status(ex.getStatusCode()).body(pd);
    }

    /**
     * Handles Bean Validation failures on @RequestBody fields.
     * Returns a structured map of field → [messages] so clients know
     * exactly which fields failed and why.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ProblemDetail> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex, HttpServletRequest req) {

        Map<String, List<String>> fieldErrors = ex.getBindingResult().getFieldErrors().stream()
                .collect(Collectors.groupingBy(
                        FieldError::getField,
                        Collectors.mapping(
                                fe -> Optional.ofNullable(fe.getDefaultMessage()).orElse("invalid"),
                                Collectors.toList())));

        ProblemDetail pd = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "Request validation failed");
        pd.setTitle("Validation Error");
        pd.setInstance(URI.create(req.getRequestURI()));
        pd.setProperty("fieldErrors", fieldErrors);
        return ResponseEntity.badRequest().body(pd);
    }

    /**
     * Handles Bean Validation failures on @RequestParam / @PathVariable
     * when @Validated is active on the controller class.
     */
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ProblemDetail> handleConstraintViolation(
            ConstraintViolationException ex, HttpServletRequest req) {

        Map<String, String> fieldErrors = ex.getConstraintViolations().stream()
                .collect(Collectors.toMap(
                        cv -> {
                            String path = cv.getPropertyPath().toString();
                            int dot = path.lastIndexOf('.');
                            return dot >= 0 ? path.substring(dot + 1) : path;
                        },
                        ConstraintViolation::getMessage,
                        (a, b) -> a + "; " + b));

        ProblemDetail pd = ProblemDetail.forStatusAndDetail(
                HttpStatus.BAD_REQUEST, "Parameter validation failed");
        pd.setTitle("Validation Error");
        pd.setInstance(URI.create(req.getRequestURI()));
        pd.setProperty("fieldErrors", fieldErrors);
        return ResponseEntity.badRequest().body(pd);
    }

    /**
     * Handles type conversion failures, e.g. a non-UUID value supplied
     * for a {alertId} path variable.
     */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ProblemDetail> handleTypeMismatch(
            MethodArgumentTypeMismatchException ex, HttpServletRequest req) {

        String typeName = ex.getRequiredType() != null
                ? ex.getRequiredType().getSimpleName() : "unknown";
        String detail = String.format("Parameter '%s' must be a valid %s", ex.getName(), typeName);

        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
        pd.setTitle("Type Mismatch");
        pd.setInstance(URI.create(req.getRequestURI()));
        return ResponseEntity.badRequest().body(pd);
    }

    /**
     * Catch-all for truly unexpected errors. Logs the full stack trace here;
     * the response intentionally omits internal details to prevent information leakage.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ProblemDetail> handleGeneric(
            Exception ex, HttpServletRequest req) {

        log.error("Unhandled exception at {} {}: {}",
                req.getMethod(), req.getRequestURI(), ex.getMessage(), ex);

        ProblemDetail pd = ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "An unexpected error occurred. Please try again later.");
        pd.setTitle("Internal Server Error");
        pd.setInstance(URI.create(req.getRequestURI()));
        return ResponseEntity.internalServerError().body(pd);
    }
}
