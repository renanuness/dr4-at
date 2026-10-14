package br.com.freela.reputacao;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface EventoRepository extends JpaRepository<Evento, UUID> {
    Optional<Evento> findByEventId(UUID uuid);
}
