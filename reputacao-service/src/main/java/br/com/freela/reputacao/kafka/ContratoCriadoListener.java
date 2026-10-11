package br.com.freela.reputacao.kafka;

import br.com.freela.reputacao.ReputacaoService;
import br.com.freela.reputacao.event.ContratoConcluido;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ContratoCriadoListener {
    private final ReputacaoService service;

    public ContratoCriadoListener(ReputacaoService service) {
        this.service = service;
    }

    @KafkaListener(
            topics = "contrato.concluido",
            groupId = "reputacao-service-group"
    )
    public void listen(ContratoConcluido evento) {
        System.out.println("Evento recebido: " + evento);
        System.out.println("Contrato ID: " + evento.contratoId());
        service.registrarContratoConcluido(evento.contratoId(),evento.freelancerId(), evento.valor());
    }
}