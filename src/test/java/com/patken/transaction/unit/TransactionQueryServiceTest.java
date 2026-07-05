package com.patken.transaction.unit;

import com.patken.transaction.api.generated.dto.TransactionResponse;
import com.patken.transaction.domain.Transaction;
import com.patken.transaction.domain.TransactionStatus;
import com.patken.transaction.domain.exception.TransactionNotFoundException;
import com.patken.transaction.persistence.TransactionRepository;
import com.patken.transaction.service.TransactionQueryService;
import com.patken.transaction.service.mapper.TransactionMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pure unit test of the read path: repository + mapper mocked. Pins the not-found
 * behaviour on both single-record lookups and the sort/paging contract of the listing
 * (most-recent-first, delegating to the right repository method per status filter).
 */
class TransactionQueryServiceTest {

    private TransactionRepository repository;
    private TransactionMapper mapper;
    private TransactionQueryService service;

    @BeforeEach
    void setUp() {
        repository = mock(TransactionRepository.class);
        mapper = mock(TransactionMapper.class);
        service = new TransactionQueryService(repository, mapper);
    }

    @Test
    void getByIdReturnsMappedResponse() {
        UUID id = UUID.randomUUID();
        Transaction entity = mock(Transaction.class);
        TransactionResponse response = new TransactionResponse();
        when(repository.findById(id)).thenReturn(Optional.of(entity));
        when(mapper.toResponse(entity)).thenReturn(response);

        assertThat(service.getById(id)).isSameAs(response);
    }

    @Test
    void getByIdThrowsWhenMissing() {
        UUID id = UUID.randomUUID();
        when(repository.findById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.getById(id))
                .isInstanceOf(TransactionNotFoundException.class)
                .hasMessageContaining(id.toString());
    }

    @Test
    void getByBusinessIdReturnsMappedResponse() {
        Transaction entity = mock(Transaction.class);
        TransactionResponse response = new TransactionResponse();
        when(repository.findByBusinessId("biz-1")).thenReturn(Optional.of(entity));
        when(mapper.toResponse(entity)).thenReturn(response);

        assertThat(service.getByBusinessId("biz-1")).isSameAs(response);
    }

    @Test
    void getByBusinessIdThrowsWhenMissing() {
        when(repository.findByBusinessId("missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.getByBusinessId("missing"))
                .isInstanceOf(TransactionNotFoundException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void listWithoutStatusFilterUsesFindAllSortedByCreatedAtDesc() {
        Transaction entity = mock(Transaction.class);
        TransactionResponse response = new TransactionResponse();
        Page<Transaction> page = new PageImpl<>(List.of(entity));
        when(repository.findAll(any(Pageable.class))).thenReturn(page);
        when(mapper.toResponse(entity)).thenReturn(response);

        Page<TransactionResponse> result = service.list(null, 0, 20);

        assertThat(result.getContent()).containsExactly(response);
        Pageable expected = PageRequest.of(0, 20, Sort.by(Sort.Direction.DESC, "createdAt"));
        verify(repository).findAll(expected);
    }

    @Test
    void listWithStatusFilterDelegatesToFindByStatus() {
        Page<Transaction> page = new PageImpl<>(List.of());
        when(repository.findByStatus(eq(TransactionStatus.RECEIVED), any(Pageable.class))).thenReturn(page);

        service.list(TransactionStatus.RECEIVED, 1, 50);

        Pageable expected = PageRequest.of(1, 50, Sort.by(Sort.Direction.DESC, "createdAt"));
        verify(repository).findByStatus(TransactionStatus.RECEIVED, expected);
    }
}
