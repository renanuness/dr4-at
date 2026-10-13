package br.com.freela.auditoria;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface EventoAuditoriaRepository extends JpaRepository<EventoAuditoria, UUID> {
    Optional<EventoAuditoria> findByEventId(UUID eventId);
}
