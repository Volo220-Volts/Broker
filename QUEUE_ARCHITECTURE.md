# Архітектура черг та мультітаскінгу для брокера повідомлень

Цей документ описує структуру та архітектуру брокера черг, фокусуючись на:
- менеджменті черг і підписок,
- механізмі `inflight`,
- логіці `requeue`,
- потоковому обробленні (воркери),
- мінімальній абстракції через інтерфейси.

> Важлива заувага: тут не описано конкретну реалізацію gRPC сервісів. Натомість, архітектура будується навколо доменних інтерфейсів, які гRPC може впровадити окремо.

---

## 1. Основні доменні поняття

### 1.1. Повідомлення

`Message` містить:
- `messageId` — глобальний персистентний ідентифікатор,
- `payload` — бінарні дані,
- `headers` — метадані,
- `routingKey` — ключ маршрутизації,
- `createdAt`, `deliveryMode` (TRANSIENT / PERSISTENT).

### 1.2. Черга

`Queue` — логічна черга повідомлень.
Ключові властивості:
- `name`;
- `durable` / `autoDelete` (для persistent модель);
- `bindings` — маршрути, які пов'язують exchange з чергою;
- `state` — у пам'яті, на диску, частково спружинена;
- `inflightLimit` — поріг для `prefetch`.

### 1.3. Підписка (Subscription)

Підписка описує конкретний Consumer, який читає з черги.
Вона містить:
- `queueName`;
- `consumerId` / `consumerTag`;
- `prefetchCount`;
- `deliveryTagGenerator`;
- `inflightTracker`;
- `status` (ACTIVE / PAUSED / CLOSED).

---

## 2. Рівні архітектури

```
API / Transport Layer
  ↕
Broker Core / Queue Engine
  ↕
Persistence / Storage Layer
  ↕
Workers / Background Tasks
```

### 2.1. API / Transport Layer

ГRPC-сервіси або будь-який інший транспорт повинен працювати з абстракціями:
- `PublisherServicePort`
- `ConsumerChannel`
- `BrokerEventPublisher`

Це дозволяє тестувати ядро незалежно від gRPC.

### 2.2. Broker Core / Queue Engine

Це серце системи.

Класичні компоненти:
- `QueueManager` — керує чергами, створює/видаляє/отримує чергу.
- `ExchangeRouter` — обчислює, у які черги потрапляє повідомлення.
- `DispatchManager` / `QueueDispatcher` — штовхає готові повідомлення до споживачів.
- `SubscriptionManager` — зберігає активні підписки та оцінює `prefetch`.
- `InFlightTracker` — відстежує невідправлені/непідтверджені повідомлення.
- `RequeueManager` — обробляє повернення повідомлень до черги.

### 2.3. Persistence / Storage Layer

Це рівень, який забезпечує ``transient`` / ``persistent`` та захист від перевантаження пам'яті.

Компоненти:
- `MessageRepository` — зберігає повідомлення у БД/журнал.
- `QueueStore` — індексує повідомлення за чергою.
- `MemorySpillService` — переводить обʼєкти з RAM на диск, коли памʼяті не вистачає.

### 2.4. Workers / Background Tasks

Типи робітників:
- `DeliveryWorker` — відправляє повідомлення підписникам.
- `RequeueWorker` — перевіряє часи життя `inflight` і повертає повідомлення.
- `ConnectionCleaner` — ловить зниклі конекти та очищає стани.
- `PagingWorker` — за потреби робить spill-to-disk.

---

## 3. Механізм `inflight`

### 3.1. Сутність `inflight`

`InFlight` — це повідомлення, яке вже було віддано консюмеру, але ще не підтверджено `ACK/NACK`.

Основні правила:
- `deliveryTag` генерується на рівні **сесії/каналу**;
- `deliveryTag` є тимчасовим і не зберігається на диску;
- до `inflight` потрапляє лише після успішної доставки клієнту;
- до підтвердження повідомлення залишається в `inflightTracker`.

### 3.2. Структура `InFlightTracker`

Схема:

```java
public interface InFlightTracker {
    long reserveDeliveryTag();
    void track(long deliveryTag, QueuedMessage message);
    QueuedMessage remove(long deliveryTag);
    Collection<QueuedMessage> drainAll();
    int size();
}
```

Можлива реалізація:

```java
public class DefaultInFlightTracker implements InFlightTracker {
    private final AtomicLong counter = new AtomicLong(0);
    private final ConcurrentMap<Long, QueuedMessage> inflight = new ConcurrentHashMap<>();

    @Override
    public long reserveDeliveryTag() {
        return counter.incrementAndGet();
    }

    @Override
    public void track(long deliveryTag, QueuedMessage message) {
        inflight.put(deliveryTag, message);
    }

    @Override
    public QueuedMessage remove(long deliveryTag) {
        return inflight.remove(deliveryTag);
    }

    @Override
    public Collection<QueuedMessage> drainAll() {
        Collection<QueuedMessage> all = inflight.values();
        inflight.clear();
        return all;
    }

    @Override
    public int size() {
        return inflight.size();
    }
}
```

### 3.3. `deliveryTag` vs `messageId`

- `messageId` — глобальний, персистентний, зберігається разом з повідомленням.
- `deliveryTag` — локальний, для кожної підписки; використовується тільки для `ACK/NACK`.

Приклад:
- Consumer отримав `deliveryTag=7` для повідомлення `messageId=42`.
- Якщо канал закривається, цей `deliveryTag` скидається і всі невідправлені повідомлення ре-контекстуються.

---

## 4. `Requeue` та життєвий цикл повідомлення

### 4.1. Типова обробка

1. Producer публікує повідомлення.
2. `QueueManager` додає повідомлення до черги.
3. `DispatchManager` бачить підписку з вільним слотом за `prefetch`.
4. Повідомлення відправляється до Consumer.
5. Повідомлення потрапляє до `inflightTracker` підписки.
6. Consumer надсилає `ACK` або `NACK`.
7. Якщо `ACK` — повідомлення остаточно видаляється.
8. Якщо `NACK(requeue=true)` або канал закрився — повідомлення повертається до черги.
9. Якщо `NACK(requeue=false)` — повідомлення може піти в dead-letter або бути вилучене.

### 4.2. Requeue в разі падіння клієнта

Якщо підписник розвалюється або закриває зʼєднання, система повинна:
- очистити внутрішній стан підписки;
- зібрати всі повідомлення із `inflightTracker`;
- повернути їх у початок черги або за політикою пріоритетів.

Ці кроки часто робляться через спеціальний `ConnectionCleaner` / `SubscriptionCloser`.

### 4.3. Requeue після `NACK`

`NackAction` може мати поле `requeue`.

```java
public interface RequeueStrategy {
    void onNack(QueuedMessage message, boolean requeue);
    void onConnectionLost(QueuedMessage message);
}
```

Можливі стратегії:
- immediate requeue;
- delay + backoff;
- dead-letter routing;
- discard.

---

## 5. `prefetch` і обмеження одночасної доставки

### 5.1. Переваги prefetch

`prefetchCount` дозволяє не відправляти в однечасно більше повідомлень, ніж Consumer може обробити.
Це критично для:
- уникнення перевантаження OK-пам'яті клієнта;
- тримання контролю над `inflight`;
- балансування між споживачами.

### 5.2. Як його реалізувати

Ключові елементи:
- `prefetchCount` зберігається у `Subscription`.
- `availableSlots = prefetchCount - inflight.size()`.
- `DispatchManager` надсилає тільки `availableSlots` повідомлень.
- при отриманні `ACK/NACK` `availableSlots` звільняється.

Приклад:

```java
public class Subscription {
    private final int prefetchCount;
    private final InFlightTracker inFlightTracker;

    public boolean canSendMore() {
        return inFlightTracker.size() < prefetchCount;
    }
}
```

### 5.3. Команда `PAUSE / RESUME`

Якщо `inflight` досягає межі, підписка може отримати команду `PAUSE`.
Це дозволяє тимчасово зупинити надсилання до клієнта, поки не прийде `ACK`.

---

## 6. Воркери та мультипоточність

### 6.1. Модель потоків

В брокері корисно розділити потоки за ролями:

- `Publish thread(s)` — отримують `PublishRequest`, зберігають повідомлення та додають до черг.
- `Dispatch thread(s)` — штовхають повідомлення до споживачів.
- `Background thread(s)` — чистять `inflight`, `requeue`, роблять spill-to-disk.

### 6.2. ExecutorService як основа

Для кожної черги чи групи черг можна використовувати:

```java
ExecutorService deliveryExecutor = Executors.newFixedThreadPool(4);
ScheduledExecutorService backgroundExecutor = Executors.newSingleThreadScheduledExecutor();
```

### 6.3. Логіка `QueueDispatcher`

`QueueDispatcher` може працювати так:

```java
public class QueueDispatcher {
    private final Queue queue;
    private final SubscriptionManager subscriptionManager;
    private final ExecutorService executor;

    public void dispatch() {
        executor.execute(() -> {
            while (queue.hasReadyMessage() && subscriptionManager.hasAvailableConsumer()) {
                Subscription sub = subscriptionManager.nextAvailable();
                if (sub.canSendMore()) {
                    QueuedMessage msg = queue.poll();
                    long tag = sub.reserveDeliveryTag();
                    sub.track(tag, msg);
                    sub.sendDelivery(tag, msg);
                } else {
                    subscriptionManager.requeue(sub);
                }
            }
        });
    }
}
```

### 6.4. Потокобезпечні структури

Використовуйте:
- `ConcurrentHashMap` для підписок і `inflight`;
- `ConcurrentLinkedQueue` або власний lock-free буфер для черг;
- `AtomicLong` для генерації тегів;
- `Semaphore` / `AtomicInteger` для `prefetch`.

### 6.5. Управління конкуренцією між підписками

Якщо кілька підписників читають з однієї черги, треба балансувати доставку.
Класичні стратегії:
- round-robin;
- weighted dispatch;
- dispatch-first-available.

---

## 7. Пропоновані інтерфейси

### 7.1. `Queue` / `QueueManager`

```java
public interface QueueManager {
    QueueDescriptor createQueue(String name, QueueOptions options);
    Optional<QueueDescriptor> getQueue(String name);
    void deleteQueue(String name);
    void enqueue(Message message, String queueName);
    Optional<QueuedMessage> poll(String queueName);
    void requeue(String queueName, QueuedMessage message);
}
```

### 7.2. `SubscriptionManager`

```java
public interface SubscriptionManager {
    Subscription register(String queueName, ConsumerChannel channel, int prefetch);
    void unregister(String consumerTag);
    Optional<Subscription> nextReady(String queueName);
    boolean hasAvailableSubscriber(String queueName);
}
```

### 7.3. `ConsumerChannel`

```java
public interface ConsumerChannel {
    String consumerTag();
    boolean canAcceptMore();
    void send(MessageDelivery delivery);
    void close(String reason);
    void onAck(long deliveryTag);
    void onNack(long deliveryTag, boolean requeue);
}
```

### 7.4. `InFlightTracker`

```java
public interface InFlightTracker {
    long nextTag();
    void add(long deliveryTag, QueuedMessage message);
    QueuedMessage remove(long deliveryTag);
    Collection<QueuedMessage> drain();
}
```

### 7.5. `RequeueStrategy`

```java
public interface RequeueStrategy {
    void onNack(QueuedMessage message, boolean requeue);
    void onConsumerDisconnected(Collection<QueuedMessage> inflight);
}
```

---

## 8. Динаміка багатозадачності

### 8.1. Публікація → черга

1. Запит на публікацію надходить до `PublishPort`.
2. `PublishService` перевіряє `exchange/routingKey`.
3. `ExchangeRouter` визначає цільові черги.
4. `QueueManager.enqueue(...)` додає повідомлення до черги і/або persist store.
5. `QueueDispatcher` отримує сигнал `dispatch()`.

### 8.2. Доставка споживачу

1. `QueueDispatcher` вибирає підписку з доступними слотами.
2. Синхронно або асинхронно формує `MessageDelivery`.
3. Відправляє через `ConsumerChannel.send(...)`.
4. `ConsumerChannel` додає повідомлення до `inflightTracker`.
5. Якщо `canAcceptMore()` ще true, продовжує доставляти.

### 8.3. Підтвердження / відхилення

- `ACK`: `ConsumerChannel.onAck(tag)` → видаляє з `inflightTracker`; видаляє з черги/БД;
- `NACK(requeue)`: `ConsumerChannel.onNack(tag, true)` → викликає `RequeueManager`, message повертається у чергу;
- `NACK(false)`: message видаляється або йде в dead-letter.

### 8.4. Втрата зʼєднання

- `ConsumerChannel.close(...)` або timeout.
- `SubscriptionManager` знімає підписку.
- `inflightTracker.drain()` повертає повідомлення у `QueueManager`.
- `QueueDispatcher` знову обробляє чергу.

---

## 9. Поділ на transient / persistent

### 9.1. Persistent

- Записуємо повідомлення в БД при enqueue.
- Можемо зберегти стан черги між рестартами.
- Перезавантаження брокера відновлює невідправлені повідомлення.

### 9.2. Transient

- Повідомлення зберігаються тільки в оперативній пам'яті.
- Якщо черга переповнюється — можна робити spill-to-disk, але без гарантій на відновлення після падіння.

### 9.3. Мінімальна політика

```java
public enum DeliveryMode { TRANSIENT, PERSISTENT }
```

```java
public interface MessageStore {
    long save(Message message, DeliveryMode mode);
    Optional<QueuedMessage> load(long messageId);
    void delete(long messageId);
}
```

---

## 10. Приклад логіки `DispatchManager`

```java
public class DispatchManager {
    private final QueueManager queueManager;
    private final SubscriptionManager subscriptionManager;
    private final ExecutorService executor;

    public void tryDispatch(String queueName) {
        executor.execute(() -> {
            while (true) {
                Optional<Subscription> subscriber = subscriptionManager.nextReady(queueName);
                if (subscriber.isEmpty()) break;

                Subscription sub = subscriber.get();
                if (!sub.canAcceptMore()) break;

                Optional<QueuedMessage> maybeMessage = queueManager.poll(queueName);
                if (maybeMessage.isEmpty()) break;

                QueuedMessage message = maybeMessage.get();
                long deliveryTag = sub.nextDeliveryTag();
                sub.track(deliveryTag, message);
                sub.send(MessageDelivery.of(deliveryTag, message));
            }
        });
    }
}
```

---

## 11. Висновок

Для мінімального брокера найважливіше:
- розділити **глобальний ідентифікатор повідомлення** та **локальний delivery tag**;
- тримати `inflight` в пам'яті для кожної підписки;
- за `ACK` видаляти повідомлення, за `NACK(requeue)` або зискоєднанням — повертати назад;
- керувати `prefetch` як обмеженням для одночасних невідправлених повідомлень;
- виконувати доставку через воркери, але не шуміти велике число потоків без конролю.

Ця архітектура дозволить вам реалізувати мінімальний працездатний брокер з хорошими основами для подальшого масштабування.
