package com.patken.transaction.unit;

import com.patken.transaction.api.generated.dto.CreateTransactionRequest;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.TransactionType;
import com.patken.transaction.domain.exception.InvalidTransactionRequestException;
import com.patken.transaction.domain.exception.ReversalNotAllowedException;
import com.patken.transaction.domain.exception.TransactionNotFoundException;
import com.patken.transaction.messaging.producer.KafkaTransactionProducer;
import com.patken.transaction.observability.TransactionMetrics;
import com.patken.transaction.persistence.TransactionGateway;
import com.patken.transaction.persistence.TransactionRepository;
import com.patken.transaction.service.TransactionCommandService;
import com.patken.transaction.service.mapper.TransactionMapper;
import com.patken.transaction.service.mapper.TransactionMapperImpl;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TransactionCommandServiceTest {

    @Mock
    private TransactionRepository repository;

    @Mock
    private TransactionGateway gateway;

    @Mock
    private KafkaTransactionProducer producer;

    private final TransactionMapper mapper = new TransactionMapperImpl();

    private TransactionCommandService service;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        TransactionMetrics metrics = new TransactionMetrics(new SimpleMeterRegistry());
        service = new TransactionCommandService(repository, gateway, mapper, producer, metrics);
    }

    @Test
    void createsCreditWithDistinctAccounts() {
        CreateTransactionRequest request = creditRequest("ACC-001", "ACC-002");
        Transaction persisted = toEntity(request, TransactionStatus.RECEIVED, null);
        when(gateway.persistIdempotent(any())).thenReturn(new TransactionGateway.PersistResult(persisted, true));

        TransactionCommandService.CommandResult result = service.create(request, "corr-1");

        assertThat(result.created()).isTrue();
        assertThat(result.transaction().getBusinessId()).isEqualTo("biz-credit");
    }

    @Test
    void rejectsSameSourceAndTargetAccount() {
        CreateTransactionRequest request = creditRequest("ACC-001", "ACC-001");

        assertThatThrownBy(() -> service.create(request, "corr-1"))
                .isInstanceOf(InvalidTransactionRequestException.class);

        verify(gateway, never()).persistIdempotent(any());
    }

    @Test
    void rejectsOriginalTransactionIdOnNonReversal() {
        CreateTransactionRequest request = creditRequest("ACC-001", "ACC-002")
                .originalTransactionId(UUID.randomUUID());

        assertThatThrownBy(() -> service.create(request, "corr-1"))
                .isInstanceOf(InvalidTransactionRequestException.class);
    }

    @Test
    void rejectsReversalWithoutOriginalTransactionId() {
        CreateTransactionRequest request = reversalRequest(null, BigDecimal.valueOf(100));

        assertThatThrownBy(() -> service.create(request, "corr-1"))
                .isInstanceOf(InvalidTransactionRequestException.class);
    }

    @Test
    void rejectsReversalOfUnknownTransaction() {
        UUID originalId = UUID.randomUUID();
        CreateTransactionRequest request = reversalRequest(originalId, BigDecimal.valueOf(100));
        when(repository.findById(originalId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.create(request, "corr-1"))
                .isInstanceOf(TransactionNotFoundException.class);
    }

    @Test
    void rejectsReversalOfAReversal() {
        UUID originalId = UUID.randomUUID();
        Transaction original = new Transaction(originalId, "biz-orig", TransactionType.REVERSAL,
                TransactionStatus.COMPLETED, BigDecimal.valueOf(100), "CAD", "ACC-001", "ACC-002",
                UUID.randomUUID(), null, "corr-0");
        when(repository.findById(originalId)).thenReturn(Optional.of(original));

        CreateTransactionRequest request = reversalRequest(originalId, BigDecimal.valueOf(100));

        assertThatThrownBy(() -> service.create(request, "corr-1"))
                .isInstanceOf(ReversalNotAllowedException.class)
                .hasMessageContaining("REVERSAL of a REVERSAL");
    }

    @Test
    void rejectsReversalOfNonCompletedOriginal() {
        UUID originalId = UUID.randomUUID();
        Transaction original = creditEntity(originalId, "biz-orig", TransactionStatus.PROCESSING, BigDecimal.valueOf(100));
        when(repository.findById(originalId)).thenReturn(Optional.of(original));

        CreateTransactionRequest request = reversalRequest(originalId, BigDecimal.valueOf(100));

        assertThatThrownBy(() -> service.create(request, "corr-1"))
                .isInstanceOf(ReversalNotAllowedException.class)
                .hasMessageContaining("COMPLETED");
    }

    @Test
    void rejectsReversalWithMismatchedAmount() {
        UUID originalId = UUID.randomUUID();
        Transaction original = creditEntity(originalId, "biz-orig", TransactionStatus.COMPLETED, BigDecimal.valueOf(100));
        when(repository.findById(originalId)).thenReturn(Optional.of(original));

        CreateTransactionRequest request = reversalRequest(originalId, BigDecimal.valueOf(50));

        assertThatThrownBy(() -> service.create(request, "corr-1"))
                .isInstanceOf(ReversalNotAllowedException.class)
                .hasMessageContaining("amount");
    }

    @Test
    void rejectsReversalWithAccountsNotMirroringTheOriginal() {
        // B3 (ADR-008 amended): reversal accounts must be the original's swapped, not
        // client-chosen. Here the request keeps the original's accounts unswapped.
        UUID originalId = UUID.randomUUID();
        Transaction original = creditEntity(originalId, "biz-orig", TransactionStatus.COMPLETED, BigDecimal.valueOf(100));
        when(repository.findById(originalId)).thenReturn(Optional.of(original));

        CreateTransactionRequest request = reversalRequest(originalId, BigDecimal.valueOf(100), "ACC-001", "ACC-002");

        assertThatThrownBy(() -> service.create(request, "corr-1"))
                .isInstanceOf(ReversalNotAllowedException.class)
                .hasMessageContaining("mirror");
    }

    @Test
    void rejectsAmountWithMoreThanFourDecimalPlaces() {
        CreateTransactionRequest request = creditRequest("ACC-001", "ACC-002");
        request.setAmount(new BigDecimal("100.123456"));

        assertThatThrownBy(() -> service.create(request, "corr-1"))
                .isInstanceOf(InvalidTransactionRequestException.class)
                .hasMessageContaining("scale");
    }

    @Test
    void rejectsReversalOfAlreadyReversedOriginal() {
        UUID originalId = UUID.randomUUID();
        Transaction original = creditEntity(originalId, "biz-orig", TransactionStatus.COMPLETED, BigDecimal.valueOf(100));
        when(repository.findById(originalId)).thenReturn(Optional.of(original));
        when(repository.existsByOriginalTransactionId(originalId)).thenReturn(true);

        CreateTransactionRequest request = reversalRequest(originalId, BigDecimal.valueOf(100));

        assertThatThrownBy(() -> service.create(request, "corr-1"))
                .isInstanceOf(ReversalNotAllowedException.class)
                .hasMessageContaining("already been reversed");
    }

    @Test
    void createsValidReversal() {
        UUID originalId = UUID.randomUUID();
        Transaction original = creditEntity(originalId, "biz-orig", TransactionStatus.COMPLETED, BigDecimal.valueOf(100));
        when(repository.findById(originalId)).thenReturn(Optional.of(original));
        when(repository.existsByOriginalTransactionId(originalId)).thenReturn(false);

        CreateTransactionRequest request = reversalRequest(originalId, BigDecimal.valueOf(100));
        Transaction persisted = toEntity(request, TransactionStatus.RECEIVED, originalId);
        when(gateway.persistIdempotent(any())).thenReturn(new TransactionGateway.PersistResult(persisted, true));

        TransactionCommandService.CommandResult result = service.create(request, "corr-1");

        assertThat(result.created()).isTrue();
        assertThat(result.transaction().getOriginalTransactionId().get()).isEqualTo(originalId);
    }

    private CreateTransactionRequest creditRequest(String sourceAccount, String targetAccount) {
        return new CreateTransactionRequest(
                "biz-credit",
                com.patken.transaction.api.generated.dto.TransactionType.CREDIT,
                BigDecimal.valueOf(100),
                "CAD",
                sourceAccount,
                targetAccount
        );
    }

    /** Accounts mirror {@link #creditEntity}'s original (ACC-001 -> ACC-002), swapped, per B3/ADR-008. */
    private CreateTransactionRequest reversalRequest(UUID originalTransactionId, BigDecimal amount) {
        return reversalRequest(originalTransactionId, amount, "ACC-002", "ACC-001");
    }

    private CreateTransactionRequest reversalRequest(UUID originalTransactionId, BigDecimal amount,
                                                       String sourceAccount, String targetAccount) {
        CreateTransactionRequest request = new CreateTransactionRequest(
                "biz-reversal",
                com.patken.transaction.api.generated.dto.TransactionType.REVERSAL,
                amount,
                "CAD",
                sourceAccount,
                targetAccount
        );
        return request.originalTransactionId(originalTransactionId);
    }

    private Transaction creditEntity(UUID id, String businessId, TransactionStatus status, BigDecimal amount) {
        return new Transaction(id, businessId, TransactionType.CREDIT, status, amount, "CAD",
                "ACC-001", "ACC-002", null, null, "corr-0");
    }

    private Transaction toEntity(CreateTransactionRequest request, TransactionStatus status, UUID originalTransactionId) {
        return new Transaction(
                UUID.randomUUID(),
                request.getBusinessId(),
                TransactionType.valueOf(request.getType().name()),
                status,
                request.getAmount(),
                request.getCurrency(),
                request.getSourceAccount(),
                request.getTargetAccount(),
                originalTransactionId,
                request.getMetadata(),
                "corr-1"
        );
    }
}
