package br.com.freela.auditoria.event;

import java.time.Instant;
import java.util.UUID;

public record ContratoCancelado(UUID eventId,
                                Instant occurredAt,
                                UUID contratoId) { }

