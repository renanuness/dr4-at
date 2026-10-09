package br.com.freela.auditoria.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ContratoCriado(UUID eventId,
                             Instant occurredAt,
                             UUID contratoId,
                             UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor) { }
