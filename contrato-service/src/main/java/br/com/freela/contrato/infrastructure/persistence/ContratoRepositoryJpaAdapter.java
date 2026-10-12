package br.com.freela.contrato.infrastructure.persistence;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.model.StatusContrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import jakarta.persistence.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class ContratoRepositoryJpaAdapter implements ContratoRepository {
    private static final Logger log = LoggerFactory.getLogger(ContratoRepositoryJpaAdapter.class);
    private final SpringDataContratoRepository jpa;
    public ContratoRepositoryJpaAdapter(SpringDataContratoRepository jpa) { this.jpa = jpa; }

    @Override
    public Contrato salvar(Contrato c) {
        log.info("contrato.persistence.save.inicio contratoId={} status={}", c.id(), c.status());
        var entity = new ContratoJpaEntity(c.id(), c.clienteId(), c.freelancerId(), c.titulo(), c.valor(), c.status(), c.criadoEm());
        var saved = jpa.save(entity);

        log.info("contrato.persistence.save.sucesso contratoId={} status={}", saved.getId(), saved.getStatus());
        return toDomain(saved);
    }

    @Override
    @Transactional
    public Contrato atualizarStatus(Contrato c){
        var contratoEntity = jpa.findById(c.id()).orElseThrow();
        log.info("contrato.persistence.save.inicio contratoId={} statusAtual={} statusNovo={}", c.id(), contratoEntity.getStatus(),c.status());

        try {
            log.info("status.field.updatable={}",
                    ContratoJpaEntity.class.getDeclaredField("status")
                            .getAnnotation(Column.class).updatable());
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        contratoEntity.setStatus(c.status());
        jpa.updateStatus(StatusContrato.ENTREGA_REGISTRADA, c.id());
        jpa.flush();
        var saved = jpa.findById(c.id()).orElseThrow();

        log.info("contrato.persistence.save.sucesso contratoId={} status={}", saved.getId(), saved.getStatus());
        return toDomain(saved);
    }

    @Override
    public Optional<Contrato> buscarPorId(UUID id) {
        log.info("contrato.persistence.findById contratoId={}", id);
        return jpa.findById(id).map(this::toDomain);
    }

    @Override
    public List<Contrato> listar() {
        log.info("contrato.persistence.findAll");
        return jpa.findAll().stream().map(this::toDomain).toList();
    }

    private Contrato toDomain(ContratoJpaEntity e) {
        return Contrato.restaurar(e.getId(), e.getClienteId(), e.getFreelancerId(), e.getTitulo(), e.getValor(), e.getStatus(), e.getCriadoEm());
    }
}
