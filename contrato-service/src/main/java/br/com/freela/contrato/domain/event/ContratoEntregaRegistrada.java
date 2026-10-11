package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.shared.DomainEvent;

import java.time.Instant;
import java.util.UUID;

public record ContratoEntregaRegistrada(UUID eventId,
                                        Instant occurredAt,
                                        UUID contratoId,
                                        UUID clienteId)
        implements DomainEvent {
    public static ContratoEntregaRegistrada novo(Contrato c) {
        return new ContratoEntregaRegistrada(UUID.randomUUID(), Instant.now(), c.id(), c.clienteId());
    }
    @Override public String eventType() { return "EntregaRegistrada"; }
    @Override public String topic(){ return "contrato.entregaRegistrada"; }
}
