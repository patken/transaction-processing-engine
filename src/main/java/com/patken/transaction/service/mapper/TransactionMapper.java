package com.patken.transaction.service.mapper;

import com.patken.transaction.api.generated.dto.TransactionResponse;
import com.patken.transaction.domain.Transaction;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.openapitools.jackson.nullable.JsonNullable;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;

/**
 * Entity -> response DTO only. Request -> entity is not mapped here: too many fields
 * on {@link Transaction} (id, initial status, correlationId) come from service logic
 * rather than the request body, so the service builds the entity via its constructor.
 */
@Mapper(componentModel = "spring")
public interface TransactionMapper {

    @Mapping(target = "transactionId", source = "id")
    @Mapping(target = "originalTransactionId", source = "originalTransactionId", qualifiedByName = "toJsonNullableUuid")
    @Mapping(target = "metadata", source = "metadata", qualifiedByName = "toJsonNullableMetadata")
    TransactionResponse toResponse(Transaction transaction);

    @Named("toJsonNullableUuid")
    default JsonNullable<UUID> toJsonNullableUuid(UUID value) {
        return value == null ? JsonNullable.undefined() : JsonNullable.of(value);
    }

    @Named("toJsonNullableMetadata")
    default JsonNullable<Map<String, Object>> toJsonNullableMetadata(Map<String, Object> value) {
        return value == null ? JsonNullable.undefined() : JsonNullable.of(value);
    }

    default OffsetDateTime map(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }
}
