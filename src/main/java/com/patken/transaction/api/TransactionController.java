package com.patken.transaction.api;

import com.patken.transaction.api.generated.TransactionsApi;
import com.patken.transaction.api.generated.dto.CreateTransactionRequest;
import com.patken.transaction.api.generated.dto.TransactionPage;
import com.patken.transaction.api.generated.dto.TransactionResponse;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.observability.MdcKeys;
import com.patken.transaction.service.TransactionCommandService;
import com.patken.transaction.service.TransactionQueryService;
import org.slf4j.MDC;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.UUID;

@RestController
public class TransactionController implements TransactionsApi {

    private final TransactionCommandService commandService;
    private final TransactionQueryService queryService;

    public TransactionController(TransactionCommandService commandService, TransactionQueryService queryService) {
        this.commandService = commandService;
        this.queryService = queryService;
    }

    @Override
    public ResponseEntity<TransactionResponse> createTransaction(CreateTransactionRequest createTransactionRequest) {
        // Established by CorrelationIdFilter for every request; travels with the transaction
        // into Kafka and on to the consumer (spec §Observability).
        String correlationId = MDC.get(MdcKeys.CORRELATION_ID);

        TransactionCommandService.CommandResult result = commandService.create(createTransactionRequest, correlationId);
        HttpStatus status = result.created() ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(result.transaction());
    }

    @Override
    public ResponseEntity<TransactionResponse> getTransactionById(UUID transactionId) {
        return ResponseEntity.ok(queryService.getById(transactionId));
    }

    @Override
    public ResponseEntity<TransactionResponse> getTransactionByBusinessId(String businessId) {
        return ResponseEntity.ok(queryService.getByBusinessId(businessId));
    }

    @Override
    public ResponseEntity<TransactionPage> listTransactions(
            com.patken.transaction.api.generated.dto.TransactionStatus status, Integer page, Integer limit) {
        TransactionStatus domainStatus =
                status == null ? null : TransactionStatus.valueOf(status.name());

        Page<TransactionResponse> results = queryService.list(domainStatus, page, limit);

        URI next = results.hasNext()
                ? ServletUriComponentsBuilder.fromCurrentRequestUri()
                        .replaceQueryParam("page", page + 1)
                        .replaceQueryParam("limit", limit)
                        .build()
                        .toUri()
                : null;

        return ResponseEntity.ok(new TransactionPage(results.getContent(), results.getTotalElements(), next));
    }
}
