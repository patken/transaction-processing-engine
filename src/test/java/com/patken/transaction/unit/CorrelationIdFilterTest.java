package com.patken.transaction.unit;

import com.patken.transaction.observability.CorrelationIdFilter;
import com.patken.transaction.observability.MdcKeys;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationIdFilterTest {

    private final CorrelationIdFilter filter = new CorrelationIdFilter();

    @Test
    void reusesAnInboundCorrelationIdHeader() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(MdcKeys.CORRELATION_ID_HEADER, "caller-supplied-id");
        MockHttpServletResponse response = new MockHttpServletResponse();

        AtomicReference<String> mdcDuringChain = new AtomicReference<>();
        filter.doFilter(request, response, (req, res) -> mdcDuringChain.set(MDC.get(MdcKeys.CORRELATION_ID)));

        assertThat(mdcDuringChain.get()).isEqualTo("caller-supplied-id");
        assertThat(response.getHeader(MdcKeys.CORRELATION_ID_HEADER)).isEqualTo("caller-supplied-id");
        assertThat(MDC.get(MdcKeys.CORRELATION_ID)).as("MDC cleared after the request").isNull();
    }

    @Test
    void generatesACorrelationIdWhenNoneSupplied() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();

        AtomicReference<String> mdcDuringChain = new AtomicReference<>();
        filter.doFilter(request, response, (req, res) -> mdcDuringChain.set(MDC.get(MdcKeys.CORRELATION_ID)));

        assertThat(mdcDuringChain.get()).isNotBlank();
        assertThat(response.getHeader(MdcKeys.CORRELATION_ID_HEADER)).isEqualTo(mdcDuringChain.get());
    }
}
