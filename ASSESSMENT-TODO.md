# Pontos deixados para os alunos

## Comunicação por mensagens
- Definir tópicos e contratos de eventos.
- Implementar producer no `contrato-service`.
- Implementar consumidores nos serviços interessados.
- Explicar por que o fluxo é assíncrono.

## Concorrência e ordenação
- Definir a key da mensagem para manter eventos do mesmo `contratoId` na mesma partição.
- Configurar concorrência de consumidores sem quebrar a ordem dentro da partição.

## Duplicidade
- Usar `eventId` como identificador idempotente.
- Demonstrar que uma mesma mensagem processada novamente não altera o resultado duas vezes.

## Mensagens transacionais
- Implementar Outbox no `contrato-service`.
- Garantir atomicidade entre persistência do Aggregate e registro do evento para publicação.

## Observabilidade
- Propagar correlation/trace context pelas mensagens.
- Centralizar logs dos microsserviços.
- Adicionar tracing distribuído e Zipkin.
- Demonstrar um fluxo completo a partir de um contrato.

## Testes
- Testar producer/consumer.
- Testar duplicidade.
- Testar ordenação.
- Testar integração com broker usando estratégia adequada para testes.


## TODO:

### GERAL
- [ ] Criar documentação para o contrato de todos os eventos

### Contrato service
- [ ] Adicionar eventos para cada ação do contrato
- [ ] Adicionar logs
- [ ] Adicionar zipin

### Notificação service
- [ ] Adicionar logs para todos os eventos recebidos
- [ ] Adicionar logs
- [ ] Adicionar zipkin

### Reputaçao service
- [ ] Consumir os eventos que sejam relacionados a reputaçao do freelance
- [ ] Persistir as alterações no banco de dados
- [ ] Adicionar zipkin

### Auditoria service
- Consumir todos os eventos
- [ ] Adicionar logs
- [ ] Adicionar zipkin