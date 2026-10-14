package br.com.freela.auditoria.kafka;

import br.com.freela.auditoria.AuditoriaService;
import br.com.freela.auditoria.EventoAuditoriaRepository;
import br.com.freela.auditoria.event.*;
import br.com.freela.auditoria.helper.CustomJsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;

@Component
public class KafkaEventListener {
    private final AuditoriaService service;
    private static final Logger log= LoggerFactory.getLogger(KafkaEventListener.class);
    private final EventoAuditoriaRepository eventoRepository;

    public KafkaEventListener(AuditoriaService service, EventoAuditoriaRepository eventoRepository) {
        this.service = service;
        this.eventoRepository = eventoRepository;
    }

    @KafkaListener(topics = "contrato.criado", groupId = "auditoria-service-group")
    public void listenContratoCriado(ConsumerRecord<String, String> record) {
        try {

            var type = CustomJsonMapper.instance()
                    .getTypeFactory()
                    .constructParametricType(EventEnvelope.class, ContratoCriado.class);
            EventEnvelope<ContratoCriado> envelope = CustomJsonMapper.instance()
                    .readValue(record.value(), type);

            if(eventoRepository.findByEventId(envelope.eventId()).isPresent()){
                log.info("Evento já processado");
                return;
            }

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
            groupId = "auditoria-service-group"
    )
    public void listenEntregaRegistrada(ConsumerRecord<String, String> record) {
        try {
            var type = CustomJsonMapper.instance()
                    .getTypeFactory()
                    .constructParametricType(EventEnvelope.class, ContratoCriado.class);
            EventEnvelope<ContratoEntregaRegistrada> envelope = CustomJsonMapper.instance()
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
            topics = "contrato.concluido",
            groupId = "auditoria-service-group"
    )
    public void listenContratoConcluido(ConsumerRecord<String, String> record) {
        try {
            var type = CustomJsonMapper.instance()
                    .getTypeFactory()
                    .constructParametricType(EventEnvelope.class, ContratoCriado.class);
            EventEnvelope<ContratoConcluido> envelope = CustomJsonMapper.instance()
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
            topics = "contrato.cancelado",
            groupId = "auditoria-service-group"
    )
    public void listenContratoCancelado(ConsumerRecord<String, String> record) {
        try {
            var type = CustomJsonMapper.instance()
                    .getTypeFactory()
                    .constructParametricType(EventEnvelope.class, ContratoCriado.class);
            EventEnvelope<ContratoCancelado> envelope = CustomJsonMapper.instance()
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
}
