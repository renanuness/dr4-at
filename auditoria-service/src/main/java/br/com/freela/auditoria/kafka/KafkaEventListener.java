package br.com.freela.auditoria.kafka;

import br.com.freela.auditoria.AuditoriaService;
import br.com.freela.auditoria.event.*;
import br.com.freela.auditoria.helper.CustomJsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;

@Component
public class Kafkaistener {
    private final AuditoriaService service;
    private static final Logger log= LoggerFactory.getLogger(Kafkaistener.class);

    public Kafkaistener(AuditoriaService service) {
        this.service = service;
    }

    @KafkaListener(
            topics = "contrato.criado",
            groupId = "auditoria-service-group"
    )
    public void listenContratoCriado(EventEnvelope<ContratoCriado> evento) {

        try {
            var eventString = CustomJsonMapper.instance().writeValueAsString(evento.payload());
            service.registrar(evento.eventId(), evento.aggregateId(), evento.eventType(), evento.correlationId().toString(), eventString);
        } catch (JacksonException e) {
            log.error("Erro serializando evento", evento);
            log.error(e.getMessage());
        }catch (Exception e){
            log.error(e.getMessage());
        }
    }

    @KafkaListener(
            topics = "contrato.entregaRegistrada",
            groupId = "auditoria-service-group"
    )
    public void listenEntregaRegistrada(ContratoEntregaRegistrada evento) {
        System.out.println("Evento recebido: " + evento);
        System.out.println("Contrato ID: " + evento.contratoId());
    }

    @KafkaListener(
            topics = "contrato.concluido",
            groupId = "auditoria-service-group"
    )
    public void listen(ContratoConcluido evento) {
        System.out.println("Evento recebido: " + evento);
        System.out.println("Contrato ID: " + evento.contratoId());
    }

    @KafkaListener(
            topics = "contrato.cancelado",
            groupId = "auditoria-service-group"
    )
    public void listen(ContratoCancelado evento) {
        System.out.println("Evento recebido: " + evento);
        System.out.println("Contrato ID: " + evento.contratoId());
    }
}