package br.com.freela.auditoria.kafka;

import br.com.freela.auditoria.event.ContratoCriado;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ContratoCriadoListener {

    @KafkaListener(
            topics = "contrato.criado",
            groupId = "notificacao-service-group"
    )
    public void listen(ContratoCriado evento) {
        System.out.println("Evento recebido: " + evento);
        System.out.println("Contrato ID: " + evento.contratoId());
        System.out.println("Cliente ID: " + evento.clienteId());

    }
}