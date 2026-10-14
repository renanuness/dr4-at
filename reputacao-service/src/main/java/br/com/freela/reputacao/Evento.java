package br.com.freela.reputacao;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

@Entity
@Table(name = "evento")
public class Evento {
    @Id
    @GeneratedValue
    private UUID id;
    private UUID eventId;

    public Evento(){}

    public Evento(UUID id, UUID eventId){
        this.id = id;
        this.eventId = eventId;
    }

    public static Evento novo(UUID uuid) {
        return new Evento(UUID.randomUUID(), uuid);
    }
}
