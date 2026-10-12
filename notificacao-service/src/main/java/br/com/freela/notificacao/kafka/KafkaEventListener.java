package br.com.freela.notificacao.kafka;

import br.com.freela.notificacao.event.ContratoCancelado;
import br.com.freela.notificacao.event.ContratoConcluido;
import br.com.freela.notificacao.event.ContratoCriado;
import br.com.freela.notificacao.event.ContratoEntregaRegistrada;
import br.com.freela.notificacao.helper.CustomJsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;

@Component
public class ContratoCriadoListener {

    @KafkaListener(
            topics = "contrato.criado",
            groupId = "notificacao-service-group"
    )
    public void listenContratoCriado(ConsumerRecord<String, String> record) {
        try {
            var type = CustomJsonMapper.instance()
                    .getTypeFactory()
                    .constructParametricType(EventEnvelope.class, ContratoCriado.class);
            EventEnvelope<ContratoCriado> envelope = CustomJsonMapper.instance()
                    .readValue(record.value(), type);

            String correlationId = envelope.correlationId() != null
                    ? envelope.correlationId()
                    : "sem-correlation-id";

            var payloadString = CustomJsonMapper.instance()
                    .writeValueAsString(envelope.payload());

            service.registrar(
                    envelope.eventId(),
                    envelope.aggregateId(),
                    envelope.eventType(),
                    correlationId,
                    payloadString
            );

            log.info("Evento auditado eventId={} eventType={} correlationId={}",
                    envelope.eventId(), envelope.eventType(), correlationId);

        } catch (JacksonException e) {
            log.error("Erro desserializando evento Kafka: {}", record.value(), e);
        } catch (Exception e) {
            log.error("Erro processando evento Kafka: {}", record.value(), e);
        }
    }

    @KafkaListener(
            topics = "contrato.entregaRegistrada",
            groupId = "notificacao-service-group"
    )
    public void listen(ContratoEntregaRegistrada evento) {
        System.out.println("Evento recebido: " + evento);
        System.out.println("Contrato ID: " + evento.contratoId());
    }

    @KafkaListener(
            topics = "contrato.concluido",
            groupId = "notificacao-service-group"
    )
    public void listen(ContratoConcluido evento) {
        System.out.println("Evento recebido: " + evento);
        System.out.println("Contrato ID: " + evento.contratoId());
    }

    @KafkaListener(
            topics = "contrato.cancelado",
            groupId = "notificacao-service-group"
    )
    public void listen(ContratoCancelado evento) {
        System.out.println("Evento recebido: " + evento);
        System.out.println("Contrato ID: " + evento.contratoId());
    }
}