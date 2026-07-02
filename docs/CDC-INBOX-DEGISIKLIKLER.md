# Polling Outbox → Debezium CDC + Inbox Pattern — Değişiklik Özeti

Bu doküman, e-ticaret sisteminde yapılan iki büyük değişikliği özetler:

1. **Producer tarafı:** `order-service`'te polling ile çalışan Transactional Outbox,
   **Debezium (CDC)** ile değiştirildi.
2. **Consumer tarafı:** `product-service`'e, at-least-once teslimatta çift işlemeyi önlemek için
   **Inbox pattern** (idempotency) eklendi.

Ayrıca yol boyunca çıkan bir **reserved-header** hatası bulunup düzeltildi.

---

## Mimari (son durum)

```
order-service ──(aynı DB transaction)──> Postgres(orderdb): orders + outbox_events
                                                │  (WAL — transaction log)
                                                ▼
                                   Debezium / Kafka Connect
                                     (Outbox Event Router SMT)
                                                │
                                                ▼
                                   Kafka topic: order-created
                                      (value = JSON, header: eventId)
                                                │
                                                ▼
product-service ──(aynı DB transaction)──> Postgres(productdb): products + processed_messages
        (Inbox: eventId ile dedup → stok yalnızca bir kez düşer)
```

**Neden?** Polling (eski `@Scheduled` relay) DB'yi periyodik tarıyor, gecikme (15 sn) ve gereksiz
sorgu yükü getiriyordu. Debezium, Postgres'in WAL'ini okuyup değişiklikleri ~anında yayınlar; tarama
yoktur.

---

## 1) order-service — Polling Outbox yerine Debezium CDC

### Altyapı (`infra/`)
| Dosya | Değişiklik |
|---|---|
| `podman-compose.yml` | Postgres'e `wal_level=logical` (+ `max_wal_senders`, `max_replication_slots`); `postgres-init` mount; **`connect`** servisi (Kafka Connect + Debezium, REST `:8083`); **`connect-register`** yardımcısı (connector'ı otomatik + idempotent kaydeder). |
| `postgres-init/01-create-orderdb.sql` | order-service için `orderdb` veritabanını oluşturur (ilk açılışta). |
| `debezium/order-outbox-connector.json` | Debezium Postgres connector + **Outbox Event Router** SMT: `aggregatetype` → `order-created` topic'i, `payload` → mesaj value (`expand.json.payload=true`), event id → **`eventId`** header'ı. |

### Kod
| Dosya | Değişiklik |
|---|---|
| `pom.xml` | H2, `spring-boot-h2console`, `spring-cloud-stream(-binder-kafka)` **kaldırıldı**; `postgresql` **eklendi**. Kafka'ya artık Debezium yazıyor, servis değil. |
| `application.yml` | Datasource H2 → **Postgres `orderdb`**; `spring.cloud.stream` ve `outbox.poller` ayarları kaldırıldı. |
| `entities/Order.java` | In-memory POJO → **JPA `@Entity`** (`orders` tablosu). |
| `repositories/OrderRepository.java` | In-memory `List` → **`JpaRepository<Order, Integer>`**. |
| `outbox/OutboxEvent.java` | Debezium convention'ına göre **insert-only** sadeleştirildi: `id (UUID)`, `aggregatetype`, `aggregateid`, `type`, `payload`, `createdat`. `status`/`retryCount`/`publishedAt`/`destination` **kaldırıldı**. |
| `outbox/OutboxRepository.java` | `findByStatus...` sorgusu kaldırıldı; sade `JpaRepository`. |
| `outbox/OutboxService.java` | `record(...)` sadeleşti (`destination` parametresi kalktı). |
| `services/concretes/OrderManager.java` | `add()` artık **`@Transactional`**: `orders` + `outbox_events` yazımı **tek transaction'da atomik** commit olur. |
| `OrderServiceApplication.java` | `@EnableScheduling` **kaldırıldı** (polling yok). |
| `outbox/OutboxMessageRelay.java` | **Silindi** (polling relay). |
| `outbox/OutboxStatus.java` | **Silindi** (PENDING/PUBLISHED/FAILED durumu artık gereksiz). |

**Sonuç:** "Gönderildi mi" bilgisini artık uygulama değil, Kafka Connect'in commit ettiği offset temsil
eder. Outbox tablosu insert-only bir event kaynağıdır.

---

## 2) product-service — Inbox Pattern (idempotency)

Inbox'ın çalışması için yan etkinin ("işlendi" kaydıyla) aynı DB transaction'ında commit'i gerekir;
bu yüzden product-service de önce Postgres/JPA'ya taşındı.

### Kod
| Dosya | Değişiklik |
|---|---|
| `pom.xml` | `spring-boot-starter-data-jpa` + `postgresql` **eklendi**. |
| `application.yml` | **Postgres `productdb`** datasource + JPA ayarları eklendi. |
| `entities/Product.java` | In-memory POJO → **JPA `@Entity`** (`products` tablosu). |
| `repositories/ProductRepository.java` | In-memory `List` → **`JpaRepository<Product, Integer>`**. |
| `inbox/ProcessedMessage.java` | **Yeni.** `processed_messages` tablosu; `message_id` **primary key** (asıl dedup garantisi). |
| `inbox/ProcessedMessageRepository.java` | **Yeni.** `JpaRepository<ProcessedMessage, String>`. |
| `inbox/OrderCreatedInboxHandler.java` | **Yeni.** `@Transactional`: `existsById` ön-kontrolü → yan etki (**stok düşürme**) + inbox insert tek transaction'da. |
| `messaging/OrderEventConsumer.java` | `Consumer<OrderCreatedEvent>` → **`Consumer<Message<OrderCreatedEvent>>`**; dedup anahtarı **`eventId`** header'ından okunur. |

### İdempotency nasıl garanti ediliyor?
- **Dedup anahtarı:** Debezium'un `eventId` header'ı (outbox UUID) — redelivery'de sabit.
- **Asıl garanti:** `processed_messages.message_id` **primary key** constraint'i. `existsById` yalnızca
  hızlı ön-kontroldür.
- **Yarış durumu:** İki teslim ön-kontrolü geçse bile ikinci insert constraint'e takılır → transaction
  rollback (stok iki kez düşmez) → offset commit edilmez → yeniden teslimde ön-kontrol yakalar.
- **Net etki:** Teslimat at-least-once kalır, ama **yan etki tam bir kez** uygulanır.
- **Fallback:** `eventId` gelmezse deterministik iş anahtarı `OrderCreated:<orderId>` kullanılır.

---

## 3) Bulunan ve düzeltilen hata — Reserved header (`id`)

**Belirti:** Yöntem A (consumer offset reset) ile aynı event'ler yeniden oynatıldığında **stok tekrar
düşüyordu** (dedup çalışmıyordu).

**Kök neden:** Consumer, dedup anahtarını `message.getHeaders().get("id")` ile okuyordu. Ama **`id`
(ve `timestamp`) Spring Messaging'de rezerve header adlarıdır** — her mesaja framework tarafından
üretilen ve **her teslimde değişen** bir UUID atanır. Kafka'daki gerçek `id` header'ı bu isimle
okunamıyordu. Sonuç: her tüketimde farklı anahtar → hiçbir mesaj "duplicate" görünmüyor.

**Kanıt:** Kafka mesajları `id:4804b269-...` taşırken, `processed_messages`'a bambaşka UUID'ler
(`a1e7b8dd-...`) yazılmıştı (framework ID'leri).

**Düzeltme:**
- **Connector:** `transforms.outbox.table.fields.additional.placement: "id:header:eventId"` eklendi —
  Debezium event id'yi çakışmayan **`eventId`** adıyla ayrı bir header olarak da gönderir.
- **Consumer:** okunan header `id` → **`eventId`** yapıldı.
- **Doğrulama:** Kafka mesajları artık `id:<uuid>,eventId:<uuid>` taşıyor; `eventId` değeri outbox
  UUID'siyle birebir aynı ve sabit.

> **Ders:** Spring Cloud Stream / Spring Messaging'de dedup/korelasyon için `id` veya `timestamp`
> header adlarını **kullanmayın**; kendi ayrı isimli header'ınızı taşıyın.

---

## Nasıl çalıştırılır (özet)

Ayrıntı: [`infra/DEBEZIUM.md`](../infra/DEBEZIUM.md)

1. `cd infra && podman compose -f podman-compose.yml up -d` — (ilk kez değilse `orderdb` için önce `down -v`).
2. `order-service`'i başlat → Hibernate `orders` + `outbox_events` tablolarını oluşturur.
3. Connector otomatik kaydolur (`connect-register`). Manuel: `POST http://localhost:8083/connectors` (bkz. DEBEZIUM.md).
4. `product-service`'i başlat → `products` + `processed_messages` tablolarını oluşturur; bir ürün ekleyin.
5. Sipariş oluşturun → event `order-created`'a ~anında akar, stok bir kez düşer.

### İdempotency testi
1. Ürün stoğunu bilinen bir değere ayarla, `processed_messages`'ı boşalt.
2. Yeni sipariş ver → `Stock updated: ... 10 -> 8`, inbox'a 1 satır.
3. `product-service`'i durdur → consumer offset'ini `--reset-offsets --to-earliest --execute` ile başa sar → yeniden başlat.
4. Beklenen: `Duplicate OrderCreated skipped ...`, **stok 8'de kalır**, yeni inbox satırı oluşmaz.

---

## İlgili dosyalar
- Debezium çalıştırma rehberi: [`infra/DEBEZIUM.md`](../infra/DEBEZIUM.md)
- Connector konfigürasyonu: [`infra/debezium/order-outbox-connector.json`](../infra/debezium/order-outbox-connector.json)
- Compose: [`infra/podman-compose.yml`](../infra/podman-compose.yml)
