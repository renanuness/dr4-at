package br.com.freela.auditoria.shared;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

public interface DomainEvent {
    UUID eventId();
    UUID aggregateId();
    Instant occurredAt();
    @JsonProperty("eventType")
    String eventType();
    String topic();
}
