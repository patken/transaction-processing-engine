package com.patken.transaction.api;

import com.patken.transaction.api.generated.TransactionsApi;
import com.patken.transaction.api.generated.dto.CreateTransactionRequest;
import com.patken.transaction.api.generated.dto.TransactionPage;
import com.patken.transaction.api.generated.dto.TransactionResponse;
import com.patken.transaction.api.generated.dto.TransactionStatus;
import com.patken.transaction.service.TransactionCommandService;
import com.patken.transaction.service.TransactionQueryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

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
        // TODO: replace with the correlationId propagated via CorrelationIdFilter/MDC.
        String correlationId = UUID.randomUUID().toString();

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
    public ResponseEntity<TransactionPage> listTransactions(TransactionStatus status, Integer page, Integer limit) {
        // Implemented in Phase 3 (query path: pagination and filtering).
        throw new UnsupportedOperationException("listTransactions is implemented in Phase 3");
    }
}
