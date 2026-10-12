package br.com.freela.auditoria.event;

import br.com.freela.auditoria.shared.DomainEvent;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelope<T>(
        T payload,
        String eventType,
        UUID eventId,
        Instant occurredAt,
        UUID aggregateId,
        String correlationId
) {
    public static <T extends DomainEvent> EventEnvelope<T> of(T event, String correlationId) {
        return new EventEnvelope<>(
                event,
                event.eventType(),
                event.eventId(),
                event.occurredAt(),
                event.aggregateId(),
                correlationId
        );
    }
}