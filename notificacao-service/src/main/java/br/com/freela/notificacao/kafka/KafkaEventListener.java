package br.com.freela.notificacao.kafka;

import br.com.freela.notificacao.NotificacaoService;
import br.com.freela.notificacao.event.*;
import br.com.freela.notificacao.helper.CustomJsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;

@Component
public class KafkaEventListener {
    private static final Logger log = LoggerFactory.getLogger(KafkaEventListener.class);
    private final NotificacaoService service;

    public KafkaEventListener(NotificacaoService service) {
        this.service = service;
    }

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

            service.registrar(
                    envelope.payload().contratoId(),
                    envelope.payload().clienteId(),
                    envelope.eventType(),
                    "Contrato iniciado."
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
    public void listenEntregaRegistrada(ConsumerRecord<String, String> record) {
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
                    envelope.payload().contratoId(),
                    envelope.payload().clienteId(),
                    envelope.eventType(),
                    "Contrato iniciado."
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
            topics = "contrato.concluido",
            groupId = "notificacao-service-group"
    )
    public void listenContratoConcluido(ConsumerRecord<String, String> record) {
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
                    envelope.payload().contratoId(),
                    envelope.payload().clienteId(),
                    envelope.eventType(),
                    "Contrato iniciado."
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
            topics = "contrato.cancelado",
            groupId = "notificacao-service-group"
    )
    public void listenContratoCancelado(ConsumerRecord<String, String> record) {
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
                    envelope.payload().contratoId(),
                    envelope.payload().clienteId(),
                    envelope.eventType(),
                    "Contrato iniciado."
            );

            log.info("Evento auditado eventId={} eventType={} correlationId={}",
                    envelope.eventId(), envelope.eventType(), correlationId);

        } catch (JacksonException e) {
            log.error("Erro desserializando evento Kafka: {}", record.value(), e);
        } catch (Exception e) {
            log.error("Erro processando evento Kafka: {}", record.value(), e);
        }
    }
}
