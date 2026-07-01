package com.patken.transaction.config;

import org.openapitools.jackson.nullable.JsonNullableModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the module that lets Jackson (de)serialize {@code JsonNullable<T>} fields
 * (used by the generated DTOs for OpenAPI `nullable: true` properties) as their plain
 * JSON value instead of the wrapper's own {present, value} shape.
 */
@Configuration
public class JacksonConfig {

    @Bean
    public JsonNullableModule jsonNullableModule() {
        return new JsonNullableModule();
    }
}
