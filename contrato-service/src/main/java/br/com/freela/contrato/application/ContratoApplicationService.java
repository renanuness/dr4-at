package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.event.EventEnvelope;
import br.com.freela.contrato.domain.helper.CustomJsonMapper;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;

import java.util.List;
import java.util.UUID;

@Service
public class ContratoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);
    private final ContratoRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public ContratoApplicationService(ContratoRepository repository, KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd, String correlationId) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={} titulo={} valor={}",
                cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());

        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());

        log.info("contrato.dominio.criado contratoId={} status={} domainEvents={}",
                contrato.id(), contrato.status(), contrato.domainEvents().size());
        Contrato salvo = repository.salvar(contrato);

        for (DomainEvent event : contrato.pullDomainEvents()) {
            log.info("contrato.evento.pendente contratoId={} eventId={} eventType={} occurredAt={}",
                    contrato.id(), event.eventId(), event.eventType(), event.occurredAt());
            try {
                var eventEnvelope = EventEnvelope.of(event, correlationId);
                var eventString = CustomJsonMapper.instance().writeValueAsString(eventEnvelope);
                kafkaTemplate.send(event.topic(), eventString);
            } catch (JacksonException e) {
                log.error("Erro serializando evento", event);
                log.error(e.getMessage());
            }catch (Exception e){
                log.error(e.getMessage());
            }
        }

        log.info("contrato.criacao.sucesso contratoId={} clienteId={} freelancerId={} status={}",
                salvo.id(), salvo.clienteId(), salvo.freelancerId(), salvo.status());
        return salvo;
    }

    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        log.info("contrato.busca.inicio contratoId={}", id);
        var contrato = repository.buscarPorId(id).orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
        log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
        return contrato;
    }

    @Transactional
    public Contrato registrarEntrega(UUID id, String correlationId){
        var contrato = buscar(id);
        log.info("contrato.registrarEntrega.inicio contratoId={}", id);

        contrato.registrarEntrega();
        repository.atualizarStatus(contrato);

        for (DomainEvent event : contrato.pullDomainEvents()) {
            log.info("contrato.evento.pendente contratoId={} eventId={} eventType={} occurredAt={}",
                    contrato.id(), event.eventId(), event.eventType(), event.occurredAt());
            try {
                var eventEnvelope = EventEnvelope.of(event, correlationId);
                var eventString = CustomJsonMapper.instance().writeValueAsString(eventEnvelope);
                kafkaTemplate.send(event.topic(), eventString);
            } catch (JacksonException e) {
                log.error("Erro serializando evento", event);
                log.error(e.getMessage());
            }catch (Exception e){
                log.error("Erro ao publicar o evento", e);
            }
        }
        log.info("contrato.registrarEntrega.sucesso contratoId={}", id, contrato.status());

        return contrato;
    }

    @Transactional
    public Contrato cancelar(UUID id, String correlationId){
        var contrato = buscar(id);
        log.info("contrato.cancelar.inicio contratoId={}", id);

        contrato.cancelar();
        repository.atualizarStatus(contrato);

        for (DomainEvent event : contrato.pullDomainEvents()) {
            log.info("contrato.evento.pendente contratoId={} eventId={} eventType={} occurredAt={}",
                    contrato.id(), event.eventId(), event.eventType(), event.occurredAt());
            try {
                var eventEnvelope = EventEnvelope.of(event, correlationId);
                var eventString = CustomJsonMapper.instance().writeValueAsString(eventEnvelope);
                kafkaTemplate.send(event.topic(), eventString);
            } catch (JacksonException e) {
                log.error("Erro serializando evento", event);
                log.error(e.getMessage());
            }catch (Exception e){
                log.error(e.getMessage());
            }
        }
        log.info("contrato.cancelar.sucesso contratoId={}", id, contrato.status());

        return contrato;
    }

    @Transactional
    public Contrato concluir(UUID id, String correlationId){
        var contrato = buscar(id);
        log.info("contrato.concluir.inicio contratoId={}", id);

        contrato.concluir();
        repository.atualizarStatus(contrato);

        for (DomainEvent event : contrato.pullDomainEvents()) {
            log.info("contrato.evento.pendente contratoId={} eventId={} eventType={} occurredAt={}",
                    contrato.id(), event.eventId(), event.eventType(), event.occurredAt());
            try {
                var eventEnvelope = EventEnvelope.of(event, correlationId);
                var eventString = CustomJsonMapper.instance().writeValueAsString(eventEnvelope);
                kafkaTemplate.send(event.topic(), eventString);
            } catch (JacksonException e) {
                log.error("Erro serializando evento", event);
                log.error(e.getMessage());
            }catch (Exception e){
                log.error(e.getMessage());
            }
        }
        log.info("contrato.concluir.sucesso contratoId={}", id, contrato.status());

        return contrato;
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();
        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }
}
