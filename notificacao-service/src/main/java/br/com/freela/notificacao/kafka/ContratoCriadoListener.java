package br.com.freela.notificacao.kafka;

import br.com.freela.notificacao.event.ContratoCriado;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class ContratoCriadoListener {

    @KafkaListener(
            topics = "contrato.criado",
            groupId = "contrato-service-group"
    )
    public void listen(ContratoCriado evento) {
        System.out.println("Evento recebido: " + evento);
        System.out.println("Contrato ID: " + evento.contratoId());
        System.out.println("Cliente ID: " + evento.clienteId());

    }
}