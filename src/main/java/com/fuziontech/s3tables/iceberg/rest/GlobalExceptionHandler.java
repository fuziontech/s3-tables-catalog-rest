package com.fuziontech.s3tables.iceberg.rest;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.servlet.NoHandlerFoundException;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(NoHandlerFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNoHandlerFoundException(NoHandlerFoundException ex) {
        return errorResponse(ex.getMessage(), "NoHandlerFoundException", HttpStatus.NOT_FOUND.value());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleAllExceptions(Exception ex) {
        return errorResponse(ex.getMessage(), ex.getClass().getSimpleName(), HttpStatus.INTERNAL_SERVER_ERROR.value());
    }

    private ResponseEntity<Map<String, Object>> errorResponse(String message, String type, int code) {
        Map<String, Object> error = new HashMap<>();
        error.put("message", message);
        error.put("type", type);
        error.put("code", code);
        error.put("status", code);

        Map<String, Object> response = new HashMap<>();
        response.put("error", error);

        return ResponseEntity.status(code).body(response);
    }
}
