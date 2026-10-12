package br.com.freela.notificacao.event;

import java.time.Instant;
import java.util.UUID;

public record ContratoEntregaRegistrada(UUID eventId,
                                        String eventType,
                                        Instant occurredAt,
                                        UUID contratoId) {}
