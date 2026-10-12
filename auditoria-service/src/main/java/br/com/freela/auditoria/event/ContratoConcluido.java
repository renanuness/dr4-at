package br.com.freela.auditoria.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ContratoConcluido(UUID eventId,
                                String eventType,
                                UUID correlationId,
                                Instant occurredAt,
                                UUID contratoId,
                                UUID freelancerId,
                                BigDecimal valor) { }
