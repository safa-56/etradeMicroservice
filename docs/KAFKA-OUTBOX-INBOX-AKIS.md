# Kafka, Transactional Outbox, Debezium CDC ve Inbox Akisi

Bu dokuman, etrade mikroservis projesinde servisler arasi asenkron iletisimin Kafka uzerinden nasil kuruldugunu ve Kafka'nin beraberinde getirdigi temel problemler icin hangi pattern'larin kullanildigini aciklar.

> Onemli: Sistemde Kafka, outbox, Debezium connector, topic isimleri, consumer group, event payload'i veya inbox davranisini etkileyen her onemli degisiklikte bu dokuman da guncellenmelidir.

## Genel Mimari

Projede Kafka, `order-service` ile `product-service` arasindaki asenkron iletisim icin kullanilir.

Ana akis:

1. Kullanici `order-service` uzerinden yeni siparis olusturur.
2. `order-service`, siparisi `orders` tablosuna kaydeder.
3. Ayni veritabani transaction'i icinde `outbox_events` tablosuna `OrderCreated` event'i yazar.
4. Debezium, PostgreSQL WAL uzerinden `outbox_events` tablosundaki insert'i yakalar.
5. Debezium Outbox Event Router, bu kaydi Kafka'daki `order-created` topic'ine event olarak yazar.
6. `product-service`, `order-created` topic'ini dinler.
7. Gelen event icin inbox kontrolu yapilir.
8. Event daha once islenmediyse urun stogu dusulur ve `processed_messages` tablosuna islenmis mesaj kaydi atilir.
9. Ayni event tekrar gelirse stok tekrar dusulmez.

## Kafka Neden Kullaniliyor?

Kafka burada servisleri gevsek bagli hale getirmek icin kullanilir.

Siparis olustugunda `order-service` dogrudan `product-service`'e REST cagrisi yapsaydi:

- `product-service` ayakta degilse siparis olusturma akisi etkilenirdi.
- Siparis olusturma islemi stok guncelleme islemine runtime seviyesinde bagli olurdu.
- Servisler arasi senkron bagimlilik artardi.
- Trafik arttiginda servisler arasi dogrudan cagrilar daha kirilgan hale gelirdi.

Bu projede `order-service` sadece `OrderCreated` olayini uretir. `product-service` bu olaya tepki verir. Boylece siparis olusturma ile stok dusme islemleri zamansal olarak ayrilir.

## Producer Tarafi: order-service

`order-service` Kafka'ya dogrudan publish etmez. Bunun yerine transactional outbox kullanir.

Ilgili kodlar:

- `order-service/src/main/java/com/etiya/orderservice/services/concretes/OrderManager.java`
- `order-service/src/main/java/com/etiya/orderservice/outbox/OutboxService.java`
- `order-service/src/main/java/com/etiya/orderservice/outbox/OutboxEvent.java`
- `order-service/src/main/resources/application.yml`

`OrderManager.add` metodu `@Transactional` calisir. Bu transaction icinde once siparis kaydedilir:

```java
Order saved = orderRepository.save(order);
```

Ardindan ayni transaction icinde outbox kaydi olusturulur:

```java
outboxService.record(
        "Order",
        String.valueOf(saved.getId()),
        "OrderCreated",
        new OrderCreatedEvent(...));
```

Buradaki kritik nokta, siparis kaydi ile outbox kaydinin ayni veritabani transaction'i icinde yazilmasidir. Yani ya ikisi birlikte commit olur ya da ikisi birlikte rollback olur.

## Ghost Event Problemi

Ghost event, event yayini ile business data yaziminin atomik olmamasindan dogar.

Problemli senaryo:

1. Uygulama Kafka'ya `OrderCreated` event'i gonderir.
2. Sonra siparisi veritabanina yazarken hata alir veya transaction rollback olur.
3. Kafka'da "siparis olustu" event'i vardir.
4. Ama veritabaninda gercek siparis yoktur.

Bu durumda `product-service` stok dusebilir, fakat aslinda kalici bir siparis olusmamistir.

Tersi de mumkundur:

1. Siparis DB'ye yazilir.
2. Kafka'ya event gonderilirken hata olur.
3. Siparis vardir ama event yoktur.
4. `product-service` stok dusmez.

Bu problem genel olarak dual write problemidir: ayni is akisinda hem DB'ye hem Kafka'ya yazmaya calismak, fakat ikisini tek atomik transaction olarak garanti edememek.

## Transactional Outbox Cozumu

Bu projede ghost event problemini engellemek icin transactional outbox kullanilir.

`OutboxEvent` entity'si `outbox_events` tablosuna map edilir. Kolonlar Debezium Outbox Event Router ile uyumlu olacak sekilde tasarlanmistir:

- `id`: Event id. Consumer tarafinda idempotency icin Kafka header'ina tasinir.
- `aggregatetype`: Aggregate tipi. Bu projede `Order`.
- `aggregateid`: Aggregate id. Bu projede siparis id'si.
- `type`: Event tipi. Bu projede `OrderCreated`.
- `payload`: JSON event icerigi.
- `createdat`: Event'in outbox'a yazildigi zaman.

`OutboxService.record` metodu event payload'ini JSON'a cevirir ve `outbox_events` tablosuna kaydeder.

Bu modelde `order-service` Kafka'ya yazmaz. Sadece kendi veritabanina yazar. Kafka'ya yazma sorumlulugu Debezium CDC tarafindadir.

## Debezium CDC Akisi

Debezium connector ayari:

- `infra/debezium/order-outbox-connector.json`

Connector PostgreSQL `orderdb` veritabanina baglanir ve sadece su tabloyu izler:

```json
"table.include.list": "public.outbox_events"
```

Outbox transform'u:

```json
"transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter"
```

Mapping:

- Event id: `id`
- Kafka key: `aggregateid`
- Event type: `type`
- Kafka value: `payload`
- Topic routing field: `aggregatetype`
- Kafka topic: `order-created`

Connector ayarinda topic sabit olarak `order-created` olacak sekilde tanimlanmistir:

```json
"transforms.outbox.route.topic.replacement": "order-created"
```

Outbox event id'si Kafka header'ina `eventId` adiyla eklenir:

```json
"transforms.outbox.table.fields.additional.placement": "id:header:eventId"
```

Bu `eventId`, `product-service` tarafinda duplicate consume problemine karsi inbox anahtari olarak kullanilir.

## Altyapi Ayarlari

Altyapi dosyasi:

- `infra/podman-compose.yml`

PostgreSQL, Debezium'un WAL okuyabilmesi icin logical replication ile acilir:

```yaml
wal_level=logical
```

Kafka broker KRaft modunda calisir ve host uygulamalari icin `localhost:9092` portundan erisilebilir.

Kafka Connect + Debezium servisi, PostgreSQL WAL kayitlarini okuyup Kafka'ya event yazar.

`connect-register` servisi, Kafka Connect REST API hazir oldugunda `infra/debezium/*.json` altindaki connector dosyalarini otomatik kaydeder. Connector zaten varsa HTTP 409 durumunu hata kabul etmez.

## Consumer Tarafi: product-service

`product-service`, Kafka event'lerini Spring Cloud Stream ile tuketir.

Ilgili kodlar:

- `product-service/src/main/resources/application.yml`
- `product-service/src/main/java/com/etiya/productservice/messaging/OrderEventConsumer.java`
- `product-service/src/main/java/com/etiya/productservice/inbox/OrderCreatedInboxHandler.java`
- `product-service/src/main/java/com/etiya/productservice/inbox/ProcessedMessage.java`

Binding ayari:

```yaml
spring:
  cloud:
    function:
      definition: orderCreated
    stream:
      kafka:
        binder:
          brokers: localhost:9092
      bindings:
        orderCreated-in-0:
          destination: order-created
          group: product-service
```

Bu ayar, `orderCreated` isimli function bean'ini `order-created` Kafka topic'ine baglar.

Consumer bean'i:

```java
public Consumer<Message<OrderCreatedEvent>> orderCreated(OrderCreatedInboxHandler handler)
```

Payload `OrderCreatedEvent` olarak deserialize edilir. Mesaj `Message<OrderCreatedEvent>` olarak alinir, cunku Debezium'un ekledigi `eventId` header'ina ihtiyac vardir.

`OrderEventConsumer`, `eventId` header'ini okur ve `OrderCreatedInboxHandler.handle(messageId, event)` metoduna devreder.

Header eksikse fallback olarak su deterministic key kullanilir:

```java
OrderCreated:<orderId>
```

Asil tercih edilen idempotency key ise Debezium'dan gelen outbox `id` degeridir.

## Duplicate Consume Problemi

Kafka consumer tarafinda teslimat pratikte at-least-once kabul edilmelidir. Yani ayni mesaj bazi durumlarda tekrar gelebilir.

Ornek nedenler:

- Consumer event'i isler ama offset commit etmeden kapanir.
- Consumer group rebalance olur.
- Handler hata alir ve mesaj yeniden denenir.
- Kafka Connect veya consumer retry mekanizmasi devreye girer.

Bu durumda ayni `OrderCreated` event'i tekrar islenirse stok ikinci kez dusulebilir. Bu nedenle consumer tarafinda idempotent isleme gerekir.

## Inbox Pattern Cozumu

Bu projede duplicate consume problemini cozmek icin inbox pattern kullanilir.

Inbox tablosu:

```text
processed_messages
```

Entity:

```text
product-service/src/main/java/com/etiya/productservice/inbox/ProcessedMessage.java
```

`message_id` primary key'tir:

```java
@Id
@Column(name = "message_id", nullable = false, updatable = false)
private String messageId;
```

Isleme metodu:

```text
product-service/src/main/java/com/etiya/productservice/inbox/OrderCreatedInboxHandler.java
```

`handle` metodu `@Transactional` calisir.

Akis:

1. `processed_messages` tablosunda `messageId` var mi kontrol edilir.
2. Varsa event duplicate kabul edilir ve islem yapilmaz.
3. Yoksa stok dusulur.
4. Ayni transaction icinde `processed_messages` tablosuna kayit atilir.

Kod akisi:

```java
if (processedRepository.existsById(messageId)) {
    return;
}

applyStockDecrement(event);

processedRepository.save(
        new ProcessedMessage(messageId, CONSUMER, EVENT_TYPE, Instant.now()));
```

Stok dusme ve inbox kaydi ayni transaction icindedir. Bu nedenle ya ikisi birlikte commit olur ya da ikisi birlikte rollback olur.

Gercek duplicate garantisi `message_id` primary key constraint'idir. Iki ayni mesaj ayni anda islenirse ikisi de ilk kontrolde "yok" gorebilir; fakat sadece biri `processed_messages` tablosuna insert edebilir. Digeri primary key ihlali alir ve transaction rollback olur.

## Event Payload

Producer ve consumer tarafinda `OrderCreatedEvent` ayni alanlari tasir:

- `orderId`
- `customerId`
- `productId`
- `quantity`
- `unitPrice`
- `totalPrice`
- `address`

Producer tarafi:

- `order-service/src/main/java/com/etiya/orderservice/events/OrderCreatedEvent.java`

Consumer tarafi:

- `product-service/src/main/java/com/etiya/productservice/events/OrderCreatedEvent.java`

Bu alanlar JSON payload olarak Kafka mesaj degerine yazilir.

## Bu Tasarimin Sagladigi Garantiler

Transactional outbox ile:

- Siparis DB'ye yazilmis ama event kaybolmus durumu engellenir.
- Event yayinlanmis ama siparis rollback olmus durumu engellenir.
- `order-service`, Kafka broker'a dogrudan bagimli olmadan siparis transaction'ini tamamlar.

Debezium CDC ile:

- Outbox tablosuna yazilan event'ler PostgreSQL WAL uzerinden Kafka'ya tasinir.
- Uygulama icinde polling relay veya manuel Kafka producer kodu gerekmez.

Inbox pattern ile:

- Ayni event tekrar gelse bile stok ikinci kez dusulmez.
- Business side effect ile "event islendi" kaydi ayni transaction icinde tutulur.
- Duplicate consume problemi DB seviyesinde primary key constraint ile kontrol altina alinir.

## Degisiklik Yaparken Guncellenmesi Gereken Basliklar

Asagidaki degisikliklerden biri yapildiginda bu dokuman da guncellenmelidir:

- Yeni Kafka topic eklenirse veya `order-created` topic adi degisirse.
- Yeni event tipi eklenirse.
- `OrderCreatedEvent` payload alanlari degisirse.
- Debezium connector config'i degisirse.
- `outbox_events` tablo semasi degisirse.
- `processed_messages` tablo semasi veya idempotency key mantigi degisirse.
- Consumer group adi degisirse.
- Yeni bir service Kafka event'i consume etmeye baslarsa.
- Kafka publish sorumlulugu Debezium disinda baska bir mekanizmaya tasinirsa.
- Stok dusme disinda yeni consumer side effect'leri eklenirse.
- Payment veya notification event akislari degisirse.

## 2026-07-02 Payment ve Notification Servisleri

Projeye `payment-service` ve `notification-service` eklendi.

Yeni event akisi:

1. `order-service`, siparis olusunca transactional outbox ile `OrderCreated` event'i uretir.
2. Debezium order outbox connector, bu event'i Kafka'daki `order-created` topic'ine yazar.
3. `product-service`, `order-created` topic'ini `product-service` consumer group'u ile dinler ve stok dususunu inbox pattern ile idempotent yapar.
4. `payment-service`, ayni `order-created` topic'ini `payment-service` consumer group'u ile dinler.
5. `payment-service`, `OrderCreated` event'ini inbox pattern ile isler, `payments` tablosuna odeme kaydi yazar ve ayni transaction icinde kendi `outbox_events` tablosuna `PaymentCompleted` event'i yazar.
6. Debezium payment outbox connector, `paymentdb.public.outbox_events` insert'lerini Kafka'daki `payment-completed` topic'ine yazar.
7. `notification-service`, `payment-completed` topic'ini `notification-service` consumer group'u ile dinler ve bildirim kaydini inbox pattern ile idempotent yapar.

Yeni servisler:

- `payment-service`
- `notification-service`

Yeni merkezi config path'leri:

```text
configs/payment-service/application.yml
configs/payment-service/application-local.yml
configs/payment-service/application-test.yml
configs/payment-service/application-prod.yml
configs/notification-service/application.yml
configs/notification-service/application-local.yml
configs/notification-service/application-test.yml
configs/notification-service/application-prod.yml
```

Kafka binding'leri:

```yaml
# payment-service
spring:
  cloud:
    function:
      definition: orderCreated
    stream:
      bindings:
        orderCreated-in-0:
          destination: order-created
          group: payment-service
```

```yaml
# notification-service
spring:
  cloud:
    function:
      definition: paymentCompleted
    stream:
      bindings:
        paymentCompleted-in-0:
          destination: payment-completed
          group: notification-service
```

Payment tarafinda Kafka'nin getirdigi problemler icin kullanilan pattern'lar:

- Duplicate consume problemi icin `payment-service/src/main/java/com/etiya/paymentservice/inbox/OrderCreatedInboxHandler.java` icinde inbox pattern kullanilir.
- Odeme is kurallari `payment-service/src/main/java/com/etiya/paymentservice/services/concretes/PaymentManager.java` icinde uygulanir.
- `PaymentManager`, `payments` tablosuna odeme kaydini yazar ve odeme kaydinin sonucu olarak `PaymentCompleted` outbox event'ini uretir.
- `processed_messages.message_id` primary key duplicate event'lere karsi veritabani seviyesinde son garantidir.
- Odeme kaydi ve `PaymentCompleted` outbox kaydi ayni `@Transactional` metod icinde yazilir.
- Bu nedenle odeme kaydi commit olursa event de outbox'a commit olur; odeme rollback olursa event de rollback olur.

Notification tarafinda Kafka'nin getirdigi problemler icin kullanilan pattern'lar:

- Duplicate consume problemi icin `notification-service/src/main/java/com/etiya/notificationservice/inbox/PaymentCompletedInboxHandler.java` icinde inbox pattern kullanilir.
- Bildirim kaydi ve `processed_messages` kaydi ayni transaction icinde yazilir.
- Ayni `PaymentCompleted` event'i tekrar gelirse bildirim tekrar olusturulmaz.

Payment outbox icin Debezium connector:

```text
infra/debezium/payment-outbox-connector.json
```

Bu connector `paymentdb` veritabanindaki `public.outbox_events` tablosunu izler ve event'leri `payment-completed` topic'ine yazar:

```json
"database.dbname": "paymentdb",
"transforms.outbox.route.topic.replacement": "payment-completed"
```

Yeni event payload'i:

```text
PaymentCompletedEvent
```

Alanlari:

- `paymentId`
- `orderId`
- `customerId`
- `amount`
- `status`
- `paidAt`

## 2026-07-02 Config Server Degisikligi

Projeye merkezi konfigurasyon icin `config-server` modulu eklendi.

Artik servislerin detayli runtime ayarlari kendi `src/main/resources/application.yml` dosyalarinda tutulmaz. Bu dosyalar sadece servis adini, aktif profili ve config-server baglantisini tasir:

```yaml
spring:
  application:
    name: product-service
  profiles:
    active: local
  config:
    import: configserver:http://localhost:8888
```

Merkezi konfigurasyonlar repo kokundeki `configs/` klasoru altindadir:

```text
configs/<service-name>/application.yml
configs/<service-name>/application-local.yml
configs/<service-name>/application-test.yml
configs/<service-name>/application-prod.yml
```

Ornek:

```text
configs/product-service/application-local.yml
```

`application.yml` dosyasi servis icin ortak ayarlari tutar. `application-local.yml`, `application-test.yml` ve `application-prod.yml` dosyalari sadece ilgili profil icin degisen ayarlari override eder.

Kafka consumer ayarlari da artik `product-service/src/main/resources/application.yml` icinde degil, merkezi config altindadir:

```text
configs/product-service/application-local.yml
configs/product-service/application-test.yml
configs/product-service/application-prod.yml
```

Bu nedenle `order-created` topic adi, Kafka broker adresi veya consumer group degistirilirse ilgili `configs/product-service/application-<profile>.yml` dosyalari guncellenmelidir.

Config Server, konfigurasyonlari su GitHub reposundaki `configs/{application}` path'lerinden okuyacak sekilde ayarlandi:

```text
https://github.com/safa-56/etradeMicroservice.git
```

## 2026-07-02 Keycloak JWT Authorization Degisikligi

`order-service` ve `product-service`, Keycloak tarafindan uretilen JWT token'lari dogrulayacak resource server olarak duzenlendi.

Merkezi config dosyalarinda issuer URI su realm'e baglandi:

```text
http://localhost:8090/realms/etiya-crm-project
```

Kod tarafinda eklenen role kurallari:

- `GET /api/orders/**` -> `order_read`
- `POST|PUT|DELETE /api/orders/**` -> `order_write`
- `GET /api/products/**` -> `product_read`
- `POST|PUT|DELETE /api/products/**` -> `product_write`

Security config siniflari Keycloak token'i icindeki hem `realm_access.roles` hem de `resource_access.<client>.roles` rollerini `ROLE_` authority formatina cevirir.
