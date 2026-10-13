package br.com.freela.reputacao.kafka;

import br.com.freela.reputacao.Evento;
import br.com.freela.reputacao.EventoRepository;
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
    private final EventoRepository eventoRepository;

    public KafkaEventListener(ReputacaoService service, EventoRepository eventoRepository) {
        this.service = service;
        this.eventoRepository = eventoRepository;
    }

    @KafkaListener(
            topics = "contrato.concluido",
            groupId = "reputacao-service-group"
    )
    public void listen(ContratoConcluido evento) {
        log.info("Evento recebido: " + evento);

        if(eventoRepository.findByEventId(evento.eventId()).isPresent()){
            log.info("Evento já processado");
            return;
        }

        service.registrarContratoConcluido(evento.contratoId(),evento.freelancerId(), evento.valor());
        eventoRepository.save(Evento.novo(evento.eventId()));
    }
}