package br.com.freela.contrato.domain.event;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.shared.DomainEvent;

import java.time.Instant;
import java.util.UUID;

public record ContratoCancelado(UUID eventId,
                                Instant occurredAt,
                                UUID contratoId)
        implements DomainEvent {
    public static ContratoCancelado novo(Contrato c) {
        return new ContratoCancelado(UUID.randomUUID(), Instant.now(), c.id());
    }

    @Override
    public UUID aggregateId() {
        return  contratoId;
    }

    @Override
    public String eventType() { return "ContratoCancelado"; }
    @Override
    public String topic(){ return "contrato.cancelado"; }
}
