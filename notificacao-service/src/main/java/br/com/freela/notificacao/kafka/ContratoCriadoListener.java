package br.com.freela.notificacao.kafka;

import br.com.freela.notificacao.event.ContratoCancelado;
import br.com.freela.notificacao.event.ContratoConcluido;
import br.com.freela.notificacao.event.ContratoCriado;
import br.com.freela.notificacao.event.ContratoEntregaRegistrada;
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