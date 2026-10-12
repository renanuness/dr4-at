package br.com.freela.contrato.domain.shared;
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
