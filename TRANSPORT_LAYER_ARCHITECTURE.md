# Transport Layer Architecture

## 1. Огляд і Мета

Transport Layer є проміжним шаром між зовнішніми клієнтами (через gRPC, HTTP, WebSocket) та внутрішнім двигуном брокера.

**Основне завдання**: відокремити протокольну логіку від домену черги і обробки повідомлень.

**Компоненти, з якими інтегрується:**
- `engine` (QueueManager, SubscriptionManager, DispatchManager, ConsumerChannel)
- `background` (BackgroundTaskScheduler для health checks, keep-alive detection)
- gRPC-генерований код (com.custombroker.grpc.*)

---

## 2. Об'єктна Модель: Proto → Transport → Engine

### 2.1 Proto-сутності (вже генеровані з broker.proto)

**Publisher Side:**
- `PublishRequest` (exchange, routing_key, payload, headers)
- `PublishResponse` (success, message_id, error_message)
- `PublishResponseSummary` (total_published, success)

**Consumer Side (Bidirectional Stream):**
- `ConsumerRequest` (oneof: SubscribeAction, AckAction, NackAction, KeepAlive)
  - `SubscribeAction` (queue_name, prefetch_count)
  - `AckAction` (delivery_tag, message_id)
  - `NackAction` (delivery_tag, message_id, requeue)
  - `KeepAlive` (timestamp)
- `BrokerResponse` (oneof: MessageDelivery, ChannelCommand, KeepAlive)
  - `MessageDelivery` (delivery_tag, Message)
  - `ChannelCommand` (type: CLOSE/PAUSE/RESUME, reason)
  - `KeepAlive` (timestamp)

### 2.2 Transport Internal Model

Проміжний шар трансляції, який служить "нейтральною мовою" між протоколом і engine.

**Publisher Path:**
```
PublishRequest (proto)
      ↓
[GrpcPublisherMessageConverter]
      ↓
PublishCommand (internal model)
      ↓
[TransportCommandRouter.route(PublishCommand)]
      ↓
engine: QueueManager.enqueue() → message persisted
      ↓
PublishResult (internal model)
      ↓
[GrpcPublisherMessageConverter]
      ↓
PublishResponse (proto)
```

**Consumer Path:**
```
ConsumerRequest.SubscribeAction (proto)
      ↓
[GrpcConsumerMessageConverter]
      ↓
SubscribeCommand (internal model)
      ↓
[TransportCommandRouter.route(SubscribeCommand)]
      ↓
engine: SubscriptionManager.register() → ConsumerChannel created
      ↓
[gRPC adapter binds ConsumerChannel → gRPC stream]
      ↓
DispatchManager delivers messages via ConsumerChannel
      ↓
[GrpcConsumerChannelAdapter translates to MessageDelivery]
      ↓
BrokerResponse.MessageDelivery (proto)
      ↓
sent back to client via stream
```

### 2.3 Engine Components (Existing)

| Component | Роль |
|-----------|------|
| `QueueManager` (InMemoryQueueManager) | Зберігання і poll повідомлень |
| `SubscriptionManager` (DefaultSubscriptionManager) | Управління підписками, round-robin, prefetch |
| `DispatchManager` | Асинхронна доставка повідомлень до ConsumerChannel |
| `ConsumerChannel` (interface) | Абстрактна точка доставки для клієнта |
| `InFlightTracker` | Відстеження доставлених, але не потверджених повідомлень |
| `Subscription` | Модель підписки з prefetch limit і inflight tracking |

---

## 3. Архітектура Transport Layer

### 3.1 Основні Компоненти

```
┌─────────────────────────────────────────────────────────────────┐
│                   Transport Layer                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ gRPC Protocol Adapters (protocol-specific)              │   │
│  │                                                           │   │
│  │  PublisherService (Publish, PublishStream)              │   │
│  │  ConsumerService (OpenChannel bidirectional stream)     │   │
│  │                                                           │   │
│  │  - Takes proto objects (PublishRequest, etc.)           │   │
│  │  - Delegates to TransportCommandRouter                  │   │
│  │  - Returns proto objects (PublishResponse, etc.)        │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            ↕                                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Message Converters (protocol-specific)                  │   │
│  │                                                           │   │
│  │  GrpcPublisherMessageConverter                           │   │
│  │    PublishRequest ↔ PublishCommand                       │   │
│  │    PublishResult ↔ PublishResponse                       │   │
│  │                                                           │   │
│  │  GrpcConsumerMessageConverter                            │   │
│  │    SubscribeAction ↔ SubscribeCommand                    │   │
│  │    AckAction ↔ AckCommand                                │   │
│  │    NackAction ↔ NackCommand                              │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            ↕                                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Internal Transport Model (protocol-agnostic)            │   │
│  │                                                           │   │
│  │  PublishCommand / PublishResult                         │   │
│  │  SubscribeCommand / AckCommand / NackCommand            │   │
│  │  TransportDelivery                                       │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            ↕                                     │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ TransportCommandRouter                                   │   │
│  │                                                           │   │
│  │  route(PublishCommand) → engine operations              │   │
│  │  route(SubscribeCommand) → engine operations            │   │
│  │  route(AckCommand) → engine operations                  │   │
│  │  route(NackCommand) → engine operations                 │   │
│  └──────────────────────────────────────────────────────────┘   │
│                            ↓                                     │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Engine Layer (existing)                       │
│                                                                  │
│  QueueManager, SubscriptionManager, DispatchManager            │
│  ConsumerChannel, InFlightTracker, Subscription                │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Ключові Інтерфейси

#### TransportCommandRouter
- **Відповідальність**: маршрутизація транспортних команд до engine операцій
- **Інтерфейс**:
  - `PublishResult route(PublishCommand)` — обробка публікацій
  - `void route(ConsumerCommand)` — обробка консюмерських команд
- **Реалізація буде**:
  - Залежить від QueueManager для enqueue
  - Залежить від SubscriptionManager для register/unregister
  - Залежить від DispatchManager для acknowledge/nack

#### GrpcPublisherMessageConverter
- **Відповідальність**: трансляція proto ↔ internal model для Publisher
- **Методи**:
  - `PublishCommand convert(PublishRequest)` — request → command
  - `PublishResponse convert(PublishResult)` — result → response
- **Валідація**: проверка exchange, routing_key, payload size

#### GrpcConsumerMessageConverter
- **Відповідальність**: трансляція proto ↔ internal model для Consumer
- **Методи**:
  - `SubscribeCommand convert(SubscribeAction)` — request → command
  - `AckCommand convert(AckAction)` — request → command
  - `NackCommand convert(NackAction)` — request → command
  - `MessageDelivery convert(TransportDelivery)` — delivery → response
- **Статус перевірка**: валідність delivery_tag, message_id

#### GrpcConsumerChannelAdapter
- **Відповідальність**: реалізує ConsumerChannel інтерфейс для gRPC
- **Поведінка**:
  - Утримує посилання на gRPC ResponseObserver (stream)
  - `send(MessageDelivery)` — конвертує TransportDelivery і пишe в stream
  - `onAck(tag)` / `onNack(tag)` — викликаються з engine при confirmации
  - `close(reason)` — граційне завершення стріму
- **Конкурентність**: безпечна для concurrent send() з DispatchManager

---

## 4. Потоки Даних

### 4.1 Publisher Path (Request-Response)

```
Client                          gRPC Service               Router              Engine
                                                                           
Publish(PublishRequest)
  │
  ├─────────────────────────→ GrpcPublisherService
                                  │
                                  ├─→ convert to PublishCommand
                                  │     │
                                  │     └──────────────────→ route(PublishCommand)
                                  │                              │
                                  │                              ├─→ QueueManager.enqueue()
                                  │                              │   ✓ Message stored
                                  │                              │
                                  │                              ← PublishResult
                                  │
                                  ├─→ convert to PublishResponse
                                  │
  PublishResponse ←─────────────┘
```

### 4.2 Consumer Path (Bidirectional Stream)

#### Фаза 1: Відкриття каналу і підписка

```
Client                          gRPC Service               Router              Engine
                                                                           
OpenChannel stream open
  │
  ├────────────────────→ GrpcConsumerService
                          │
                          ├─→ create GrpcConsumerChannelAdapter
                          │   (implements ConsumerChannel)
                          │
                          └─→ ready to receive SubscribeAction
```

#### Фаза 2: Отримання повідомлень (Push від Server)

```
gRPC Stream (bidirectional)

Client receives
SubscribeAction(queue, prefetch)
  │
  ├────────────────────→ GrpcConsumerService
                          │
                          ├─→ convert to SubscribeCommand
                          │     │
                          │     └──────────────────→ route(SubscribeCommand)
                          │                              │
                          │                              ├─→ SubscriptionManager.register()
                          │                              │   ✓ Subscription created
                          │                              │   ✓ ConsumerChannel = adapter
                          │
                          │                         DispatchManager (async)
                          │                              │
                          │                              ├─→ poll from queue
                          │                              │
                          │                              ├─→ track in-flight
                          │                              │
                          │                              ├─→ ConsumerChannel.send()
                          │                              │   (calls adapter.send())
                          │
                          ├─→ adapter converts TransportDelivery
                          │
  BrokerResponse ←─────┘
(MessageDelivery)
```

#### Фаза 3: Підтвердження (Ack/Nack)

```
Client sends
AckAction(delivery_tag, message_id)
  │
  ├────────────────────→ GrpcConsumerService
                          │
                          ├─→ convert to AckCommand
                          │     │
                          │     └──────────────────→ route(AckCommand)
                          │                              │
                          │                              ├─→ DispatchManager.acknowledge()
                          │                              │   ✓ Remove from inflight
                          │                              │   ✓ Trigger dispatch for new messages
```

#### Фаза 4: Keep-Alive

```
Періодично (фоновий таск):

BackgroundTaskScheduler (existing)
  │
  ├─→ KeepAliveTask
       │
       ├─→ GrpcConsumerService sends KeepAlive
       │   (via adapter.sendKeepAlive())
       │
  Client receives KeepAlive
  Client responds with Ping
```

---

## 5. Взаємодія з Існуючими Компонентами

### 5.1 Integration with Engine

| Transport Component | Engine Component | Операція |
|-------------------|------------------|----------|
| TransportCommandRouter | QueueManager | enqueue(PublishCommand) |
| TransportCommandRouter | SubscriptionManager | register(SubscribeCommand) |
| TransportCommandRouter | DispatchManager | acknowledge(AckCommand) |
| TransportCommandRouter | DispatchManager | negativeAcknowledge(NackCommand) |
| GrpcConsumerChannelAdapter | DispatchManager | send(MessageDelivery) |
| GrpcConsumerChannelAdapter | Subscription.InFlightTracker | track/remove tags |

### 5.2 Integration with Background Tasks

| Component | Task | Функція |
|-----------|------|---------|
| gRPC Stream Manager | KeepAliveTask | Periodic KeepAlive pings до clients |
| Transport Layer | ConnectionHealthTask | Виявлення мертвих З'єднань |
| Transport Layer | StreamTimeoutTask | Close streams за timeout без активності |

---

## 6. Структура Каталогів І Пакетів

```
com.volo.broker
├── transport/
│   ├── TransportLayer.java (Spring @Component)
│   ├── TransportCommandRouter.java (interface)
│   ├── TransportCommandRouterImpl.java (реалізація)
│   │
│   ├── model/
│   │   ├── PublishCommand.java
│   │   ├── PublishResult.java
│   │   ├── ConsumerCommand.java (sealed interface)
│   │   │   ├── SubscribeCommand.java
│   │   │   ├── AckCommand.java
│   │   │   └── NackCommand.java
│   │   ├── TransportDelivery.java
│   │   └── TransportException.java
│   │
│   └── grpc/
│       ├── GrpcPublisherService.java (implements PublisherServiceGrpc.PublisherServiceImplBase)
│       ├── GrpcConsumerService.java (implements ConsumerServiceGrpc.ConsumerServiceImplBase)
│       ├── GrpcPublisherMessageConverter.java (implements TransportMessageConverter)
│       ├── GrpcConsumerMessageConverter.java (implements TransportMessageConverter)
│       ├── GrpcConsumerChannelAdapter.java (implements ConsumerChannel)
│       ├── GrpcStreamManager.java (manages stream lifecycle)
│       └── GrpcExceptionHandler.java (maps exceptions to gRPC status codes)
│
├── engine/ (existing, not changed)
│   ├── QueueManager, SubscriptionManager, etc.
│   └── ConsumerChannel (interface that GrpcConsumerChannelAdapter implements)
│
└── background/ (existing, can integrate for keep-alive)
    ├── BackgroundTaskScheduler
    └── KeepAliveTask (can be added)
```

---

## 7. Відповідальність Компонентів

| Компонент | Відповідальність | НЕ відповідає за |
|-----------|-----------------|------------------|
| **TransportLayer** | Запуск/зупинка всіх transport компонентів | Бізнес-логіка черги |
| **TransportCommandRouter** | Маршрутизація команд до engine | Обробка протоколу |
| **GrpcPublisherService** | Обробка gRPC PublishRequest/PublishResponse | Збереження даних |
| **GrpcConsumerService** | Управління OpenChannel стрімом, розбір ConsumerRequest | In-flight tracking |
| **GrpcConsumerChannelAdapter** | Трансляція внутрішніх доставок у gRPC stream | Диспетчеризація |
| **GrpcMessageConverter** | Конвертація proto ↔ internal model | Валідація бізнес-правил |
| **Engine Components** | Логіка черги, in-flight, prefetch, dispatch | Протокольна специфіка |

---

## 8. Сценарії Помилок І Recovery

### 8.1 Publisher Failures

**Сценарій**: Максимальний розмір черги перевищено

```
PublishRequest received
  ↓
TransportCommandRouter.route(PublishCommand)
  ↓
QueueManager.enqueue() → throws QueueFullException
  ↓
GrpcPublisherService catches exception
  ↓
PublishResponse(success=false, error="Queue full")
```

### 8.2 Consumer Stream Failures

**Сценарій**: Consumer відключився, не закривши стрім

```
Client abruptly disconnects
  ↓
gRPC stream dies (StreamObserver.onError/onCompleted triggered)
  ↓
GrpcConsumerService cleanup:
   - Unregister subscription
   - Close GrpcConsumerChannelAdapter
   - InFlightTracker.drainAll() → requeue messages
```

**Сценарій**: Consumer не відповідає на keep-alive

```
BackgroundTask: KeepAliveTimeout check
  ↓
Stream hasn't responded for N seconds
  ↓
Close stream & unregister (same as above)
```

---

## 9. Розширення & Future-proofing

### 9.1 Додаткові Транспортні Адаптери (позніше)

```
transport/
├── grpc/          (вже є)
├── http/          (future: REST API для simple publish)
├── websocket/     (future: WebSocket для consumer streams)
└── amqp/          (future: AMQP 0-9-1 compatibility)
```

### 9.2 Додатковие Конвертери

```
transport.grpc/
├── GrpcTlsConfig.java (future: SSL/TLS support)
├── GrpcAuthInterceptor.java (future: Authentication/Authorization)
└── GrpcMetricsInterceptor.java (future: Latency/throughput metrics)
```

### 9.3 Backpressure & Flow Control

```
GrpcConsumerChannelAdapter:
  ├─ Track pending sends (to avoid memory bloat)
  ├─ Pause dispatch if buffer full
  └─ Resume when client catches up
```

---

## 10. Дизайнові Рішення

### 10.1 Чому proto Objects не використовуються напряму в Engine?

- **Proto-generated classes тісно пов'язані з gRPC** — зміна одного впливає на інше
- **Engine повинен бути transport-agnostic** — в майбутньому можливі HTTP, WebSocket тощо
- **Internal model (PublishCommand, etc.) дозволяє:**
  - Незалежно еволюціонувати протокол і engine
  - Легко додавати валідацію/трансформацію
  - Тестувати engine без gRPC

### 10.2 Чому GrpcConsumerChannelAdapter реалізує ConsumerChannel?

- **Adapter Pattern**: робить gRPC stream сумісним з engine контрактом
- **DispatchManager не знає про gRPC** — просто викликає `channel.send()`
- **Незалежна еволюція**: нові адаптери можуть реалізувати той же інтерфейс

### 10.3 Чому Bidirectional Stream для Consumer, а не Unary для Producer?

- **Publisher**: одиночна публікація = Unary RPC (просто & fast)
- **Consumer**: довгоживу підписку з push notifications = Bidirectional Stream
  - Клієнт може надіслати ACK/NACK "когда доведется"
  - Брокер активно штовхає повідомлення
  - Keep-alive піни = гарний signal про активність

---

## 11. Вимоги до Реалізації (Checklist для майбутніх розробників)

### Functional Requirements
- [ ] TransportCommandRouter маршрутизує PublishCommand до engine
- [ ] TransportCommandRouter маршрутизує SubscribeCommand до engine
- [ ] GrpcPublisherService обробляє Publish & PublishStream
- [ ] GrpcConsumerService обробляє OpenChannel bidirectional stream
- [ ] GrpcConsumerChannelAdapter імплементує ConsumerChannel
- [ ] Message converters коректно трансляють proto ↔ internal model
- [ ] AckCommand / NackCommand викликають engine acknowledge/nack методи

### Non-Functional Requirements
- [ ] Concurrency: GrpcConsumerChannelAdapter.send() thread-safe
- [ ] Error Handling: exceptions конвертуються в gRPC Status codes
- [ ] Keep-Alive: periodic pings запобігають socket timeout
- [ ] Backpressure: adapter не переповнює memory при rapid sends
- [ ] Logging: INFO on subscribe/unsubscribe, DEBUG on each delivery

---

## 12. Залежності & Integration Points

### Internal Dependencies
- `engine.QueueManager` — enqueue/poll messages
- `engine.SubscriptionManager` — register/unregister consumers
- `engine.DispatchManager` — acknowledge/nack, trigger dispatch
- `engine.ConsumerChannel` — interface to implement
- `background.BackgroundTaskScheduler` — keep-alive & health checks

### External Dependencies (gRPC Generated)
- `com.custombroker.grpc.PublisherServiceGrpc`
- `com.custombroker.grpc.ConsumerServiceGrpc`
- `com.custombroker.grpc.PublishRequest` / `PublishResponse`
- `com.custombroker.grpc.ConsumerRequest` / `BrokerResponse`
- `com.custombroker.grpc.Message` / `MessageDelivery`

### Spring Integration
- `@Service`, `@Component` annotations
- Constructor injection for dependencies
- Spring lifecycle (`DisposableBean` for cleanup)

---

**Висновок**: Архітектура Transport Layer розділяє concerns між протоколом (gRPC) та двигуном, дозволяючи незалежну еволюцію та легкість тестування. Готові proto-сутності з broker.proto стають точками інтеграції з GrpcConsumerChannelAdapter та GrpcPublisherService.