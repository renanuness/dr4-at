package br.com.freela.notificacao.event;

import java.time.Instant;
import java.util.UUID;

public record ContratoCancelado(UUID eventId,
                                Instant occurredAt,
                                UUID contratoId){ }
