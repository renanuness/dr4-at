package br.com.freela.contrato.infrastructure.persistence;
import br.com.freela.contrato.domain.model.StatusContrato;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.UUID;

interface SpringDataContratoRepository extends JpaRepository<ContratoJpaEntity, UUID> {
    @Modifying
    @Query("update ContratoJpaEntity set status = ?1 where id = ?2")
    void updateStatus(StatusContrato status, UUID id);
}
