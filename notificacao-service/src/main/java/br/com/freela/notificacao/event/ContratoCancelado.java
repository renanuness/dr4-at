package br.com.freela.notificacao.event;

import java.time.Instant;
import java.util.UUID;

public record ContratoCancelado(UUID eventId,
                                String eventType,
                                Instant occurredAt,
                                UUID contratoId){ }
