# Debezium (CDC) ile Transactional Outbox

Bu proje artık outbox tablosunu **polling** ile taramıyor. Bunun yerine **Debezium**,
PostgreSQL'in transaction log'unu (WAL) okuyup `outbox_events` tablosuna yapılan INSERT'leri
`order-created` Kafka topic'ine event olarak yazıyor.

```
order-service ──(aynı DB transaction)──> Postgres(orderdb): orders + outbox_events
                                                │  (WAL)
                                                ▼
                                   Debezium / Kafka Connect
                                                │
                                                ▼
                                   Kafka topic: order-created
                                                │
                                                ▼
product-service ──(aynı DB transaction)──> Postgres(productdb): products + processed_messages
        (Inbox pattern: id header ile dedup → stok yalnızca bir kez düşer)
```

## Bileşenler
- **Postgres** `wal_level=logical` ile çalışır (bkz. `podman-compose.yml` → `postgres.command`).
- **orderdb** veritabanı ilk açılışta `postgres-init/01-create-orderdb.sql` ile oluşturulur.
- **connect** servisi = Kafka Connect + Debezium (REST API: `http://localhost:8083`).
- **order-outbox-connector.json** = Debezium Postgres connector + Outbox Event Router (SMT) konfigürasyonu.
- **connect-register** servisi = Connect hazır olunca connector'ı otomatik kaydeden tek seferlik
  yardımcı (idempotent; connector zaten varsa HTTP 409 → sorun değil). Elle POST'a gerek bırakmaz.
- **productdb** = product-service'in DB'si (compose'da `POSTGRES_DB` ile hazır gelir); `products` ve
  `processed_messages` (Inbox) tabloları burada. Consumer idempotency'si bu tabloya dayanır.

## Çalıştırma Adımları

### 1. Altyapıyı ayağa kaldır
```powershell
cd infra
podman compose -f podman-compose.yml up -d
```
> Not: `orderdb` yalnızca **boş** bir postgres volume'ünde oluşur. Daha önce çalıştıysanız,
> veritabanını yeniden oluşturmak için önce `podman compose -f podman-compose.yml down -v`
> ile volume'leri silin.

### 2. order-service'i başlat (tabloları Hibernate oluşturur)
Connector'ı kaydetmeden ÖNCE `orders` ve `outbox_events` tablolarının var olması gerekir.
```powershell
# proje kökünden
mvn -pl order-service spring-boot:run
```

### 3. Debezium connector'ı (otomatik kaydolur)
`connect-register` servisi Connect hazır olur olmaz connector'ı kendisi POST eder — normalde
elle bir şey yapmanıza gerek yoktur. Sadece order-service tablolarını oluşturduktan sonra
yardımcıyı bir kez tetiklemek isterseniz:
```powershell
cd infra
podman compose -f podman-compose.yml up connect-register
```

İsterseniz elle de kaydedebilirsiniz:
```powershell
# proje kökünden
Invoke-RestMethod -Method Post -Uri http://localhost:8083/connectors `
  -ContentType application/json `
  -InFile infra/debezium/order-outbox-connector.json
```

Durumu kontrol:
```powershell
Invoke-RestMethod http://localhost:8083/connectors/order-outbox-connector/status
```
`connector.state` ve `tasks[].state` = **RUNNING** olmalı.

### 4. product-service'i başlat (Postgres/inbox ile)
product-service artık `productdb`'ye bağlanır; `products` ve `processed_messages` tablolarını
Hibernate oluşturur. Stok düşürme yan etkisini görebilmek için önce bir ürün oluşturun:
```powershell
mvn -pl product-service spring-boot:run
# sonra bir urun ekleyin (id'yi not edin)
Invoke-RestMethod -Method Post -Uri http://localhost:<product-port>/api/products `
  -ContentType application/json `
  -Body '{"name":"Telefon","unitPrice":100,"stock":10,"description":"demo"}'
```

### 5. Uçtan uca test
```powershell
# Yukarida olusturulan urunun id'si ile siparis ver
Invoke-RestMethod -Method Post -Uri http://localhost:<order-port>/api/orders `
  -ContentType application/json `
  -Body '{"customerId":1,"productId":1,"quantity":2,"unitPrice":50,"address":"Ankara"}'
```
Beklenen: `order-created` topic'ine bir mesaj düşer, **product-service** log'unda
`OrderCreated processed ...` ve `Stock updated: productId=1 10 -> 8` görülür. Polling gecikmesi
(eski 15 sn) yerine olay ~anında akar.

**Idempotency doğrulaması:** Aynı event'i yeniden teslim ettirin (product-service'i durdurup
`processed_messages` etkisini görmek için consumer grubunu başa sarabilir ya da connector'ı
silip `snapshot.mode=initial` ile yeniden kaydedip mevcut outbox satırlarını yeniden bastırabilirsiniz).
Beklenen: log'da `Duplicate OrderCreated skipped ...` görünür ve **stok bir daha düşmez**.

## Önemli Notlar
- **Atomiklik:** `OrderManager.add()` artık `@Transactional`; `orders` ve `outbox_events`
  yazımı tek transaction'da commit olur. Debezium yalnızca commit edilmiş WAL kayıtlarını okur.
- **Insert-only:** `outbox_events` sadece INSERT edilir; `status`/`retryCount` alanları kaldırıldı.
  "Gönderildi mi" bilgisini artık Kafka Connect'in commit ettiği offset temsil eder.
- **Topic & payload uyumu:** Outbox Event Router `aggregatetype` → `order-created` topic'ine
  yönlendirir, `payload` (JSON) mesaj value'su olur (`expand.json.payload=true`). Böylece
  product-service tarafında herhangi bir değişiklik gerekmez.
- **Idempotency (Inbox pattern):** Teslimat en-az-bir-kez olduğundan product-service **Inbox
  pattern** ile korunur. Dedup anahtarı Debezium'un **`eventId`** header'ıdır (outbox UUID); bkz.
  [OrderEventConsumer](../product-service/src/main/java/com/etiya/productservice/messaging/OrderEventConsumer.java)
  ve [OrderCreatedInboxHandler](../product-service/src/main/java/com/etiya/productservice/inbox/OrderCreatedInboxHandler.java).
  - Handler `@Transactional`: stok düşürme + `processed_messages` insert'i tek transaction'da commit olur.
  - Asıl garanti `processed_messages.message_id` **primary key** constraint'idir; `existsById` sadece
    hızlı ön-kontroldür. Yarış durumunda ikinci insert constraint'e takılır → transaction rollback →
    offset commit edilmez → yeniden teslimde ön-kontrol yakalar. Net etki: yan etki **tam bir kez**.
  - `eventId` header'ı bir sebeple gelmezse deterministik iş anahtarına (`OrderCreated:<orderId>`) düşülür.
  - **Neden `id` değil `eventId`?** Spring Messaging'de `id` (ve `timestamp`) header adları
    **rezervedir**; her mesaja framework tarafından üretilen ve her teslimde değişen bir UUID atanır.
    Kafka'daki `id` header'ı bu isimle okunamaz → dedup çalışmaz. Bu yüzden Debezium event id'yi
    `transforms.outbox.table.fields.additional.placement: "id:header:eventId"` ile ayrıca `eventId`
    adıyla gönderir ve consumer bunu okur.
- **Temizlik (opsiyonel):** hem `outbox_events` hem `processed_messages` zamanla büyür.
  `outbox_events` WAL'den okunduğu için serbestçe silinebilir. `processed_messages` için ise
  **retention penceresi, mümkün olan en büyük yeniden-teslim gecikmesinden uzun olmalı** — çok erken
  silinen bir kayıt, gecikmeli bir duplicate'in tekrar işlenmesine yol açabilir.
