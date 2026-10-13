package br.com.freela.reputacao.kafka;

import br.com.freela.reputacao.ReputacaoService;
import br.com.freela.reputacao.event.ContratoConcluido;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaEventListener {
    private final ReputacaoService service;
    private static final Logger log = LoggerFactory.getLogger(KafkaEventListener.class);

    public KafkaEventListener(ReputacaoService service) {
        this.service = service;
    }

    @KafkaListener(
            topics = "contrato.concluido",
            groupId = "reputacao-service-group"
    )
    public void listen(ContratoConcluido evento) {
        log.info("Evento recebido: " + evento);
        service.registrarContratoConcluido(evento.contratoId(),evento.freelancerId(), evento.valor());
    }
}