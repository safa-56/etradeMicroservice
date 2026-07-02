-- Servisler icin ayri veritabanlari.
-- Bu script SADECE Postgres ilk kez (bos volume ile) ayaga kalkarken calisir.
-- Mevcut bir volume varsa once "podman compose down -v" ile silinmelidir.
CREATE DATABASE orderdb;
CREATE DATABASE paymentdb;
CREATE DATABASE notificationdb;
