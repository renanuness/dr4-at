package br.com.freela.auditoria.kafka;

import br.com.freela.auditoria.event.ContratoCancelado;
import br.com.freela.auditoria.event.ContratoConcluido;
import br.com.freela.auditoria.event.ContratoCriado;
import br.com.freela.auditoria.event.ContratoEntregaRegistrada;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ContratoCriadoListener {

    @KafkaListener(
            topics = "contrato.criado",
            groupId = "auditoria-service-group"
    )
    public void listenContratoCriado(ContratoCriado evento) {
        System.out.println("Evento recebido: " + evento);
        System.out.println("Contrato ID: " + evento.contratoId());
        System.out.println("Cliente ID: " + evento.clienteId());

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