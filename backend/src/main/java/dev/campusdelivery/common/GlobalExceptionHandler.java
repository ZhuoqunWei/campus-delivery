package dev.campusdelivery.common;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ProblemDetail handleApi(ApiException ex) {
        return problem(ex.status(), ex.code(), ex.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        Map<String, String> fields = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(fe -> fields.putIfAbsent(fe.getField(), fe.getDefaultMessage()));
        ProblemDetail pd = problem(HttpStatus.BAD_REQUEST, "validation_failed", "Request validation failed");
        pd.setProperty("fields", fields);
        return pd;
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ProblemDetail handleMissingHeader(MissingRequestHeaderException ex) {
        return problem(HttpStatus.BAD_REQUEST, "missing_header",
                "Missing required header: " + ex.getHeaderName());
    }

    /** Database-enforced invariants surface here: illegal transitions, duplicate active assignment, ... */
    @ExceptionHandler(DataAccessException.class)
    public ProblemDetail handleDataAccess(DataAccessException ex) {
        String state = PgErrors.sqlState(ex);
        if (PgErrors.ILLEGAL_TRANSITION.equals(state)) {
            return problem(HttpStatus.CONFLICT, "illegal_transition", PgErrors.message(ex));
        }
        if (PgErrors.UNIQUE_VIOLATION.equals(state)) {
            return problem(HttpStatus.CONFLICT, "conflict", "The operation conflicts with the current state");
        }
        if (PgErrors.CHECK_VIOLATION.equals(state) || PgErrors.IMMUTABLE_COLUMN.equals(state)) {
            return problem(HttpStatus.CONFLICT, "invariant_violation", PgErrors.message(ex));
        }
        if (PgErrors.DEADLOCK.equals(state)) {
            return problem(HttpStatus.CONFLICT, "retry", "Concurrent update detected, please retry");
        }
        log.error("Unhandled data access error (sqlstate={})", state, ex);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "internal_error", "Unexpected database error");
    }

    private static ProblemDetail problem(HttpStatus status, String code, String detail) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(status, detail);
        pd.setType(URI.create("https://campusdelivery.dev/problems/" + code));
        pd.setTitle(code);
        pd.setProperty("code", code);
        return pd;
    }
}
