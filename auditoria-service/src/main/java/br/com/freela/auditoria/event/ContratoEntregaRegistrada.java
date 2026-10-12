package br.com.freela.auditoria.event;

import java.time.Instant;
import java.util.UUID;

public record ContratoEntregaRegistrada(UUID eventId,
                                        String eventType,
                                        UUID correlationId,
                                        Instant occurredAt,
                                        UUID contratoId){}
