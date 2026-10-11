package br.com.freela.notificacao.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record ContratoConcluido(UUID eventId,
                                Instant occurredAt,
                                UUID contratoId,
                                UUID freelancerId,
                                BigDecimal valor) {}
