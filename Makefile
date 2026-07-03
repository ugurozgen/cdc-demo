# MM2 lab — yardımcı komutlar
# Kullanım: make up / make demo / make verify / make down

A := docker compose exec -T kafka-a /opt/kafka/bin
B := docker compose exec -T kafka-b /opt/kafka/bin
BS_A := --bootstrap-server localhost:19092
BS_B := --bootstrap-server localhost:19092
# Container-içi CLI INTERNAL listener'a (localhost:19092) bağlanır; HOST listener
# (9092/9094) advertised adresi container dışına işaret ettiği için içeriden kullanılmaz.

PSQL_S := docker compose exec -T pg-source psql -U postgres -d sourcedb
PSQL_T := docker compose exec -T pg-target psql -U postgres -d targetdb
PSQL_SB := docker compose exec -T pg-source-b psql -U postgres -d sourcedb
PSQL_TB := docker compose exec -T pg-target-b psql -U postgres -d targetdb
CONNECT_A := http://localhost:8084
CONNECT_B := http://localhost:8085
CONNECT := $(CONNECT_A)

.PHONY: up down clean logs ps demo verify topics-a topics-b groups-b reset \
        cdc-register cdc-status cdc-test cdc-delete failover-check \
        failover failback active-status cdc-watch db-status db-reprovision psql-source psql-target help

up:                ## Cluster'ları + MM2'yi başlat
	docker compose up -d
	@echo "Brokerlar ayağa kalkıyor... 'make ps' ile health kontrol et."

down:              ## Durdur (volume'ler kalır)
	docker compose down

clean:             ## Durdur + tüm veriyi sil
	docker compose down -v

ps:                ## Servis durumları
	docker compose ps

logs:              ## MM2 driver loglarını izle
	docker compose logs -f mm2

# --- Demo akışı ---
demo:              ## Topic oluştur + mesaj üret + consumer group offset commit et (gebze)
	@echo ">> gebze'de 'demo' topic'i oluşturuluyor"
	-$(A)/kafka-topics.sh $(BS_A) --create --topic demo --partitions 3 --replication-factor 1
	@echo ">> gebze/demo'ya 10 mesaj üretiliyor"
	@seq 1 10 | $(A)/kafka-console-producer.sh $(BS_A) --topic demo
	@echo ">> consumer group 'demo-cg' ile 10 mesaj tüketilip offset commit ediliyor"
	$(A)/kafka-console-consumer.sh $(BS_A) --topic demo --group demo-cg \
		--from-beginning --max-messages 10 --timeout-ms 15000 || true
	@echo ">> Bitti. ~10sn bekleyip 'make verify' çalıştır (MM2 mirror+offset sync için)."

verify:            ## ankara tarafında mirror + offset translation doğrula
	@echo "================ ankara TOPIC LİSTESİ ================"
	$(B)/kafka-topics.sh $(BS_B) --list
	@echo ""
	@echo "========== ankara'da mirror edilen mesajlar (gebze.demo) =========="
	$(B)/kafka-console-consumer.sh $(BS_B) --topic gebze.demo \
		--from-beginning --max-messages 10 --timeout-ms 10000 || true
	@echo ""
	@echo "====== ankara'da consumer group offset'i (MM2 çevirdi) ======"
	$(B)/kafka-consumer-groups.sh $(BS_B) --describe --group demo-cg || true
	@echo ""
	@echo "============ MM2 internal topic'leri (ankara) ============"
	$(B)/kafka-topics.sh $(BS_B) --list | grep -E 'checkpoint|heartbeat|offset-sync|mm2' || true

topics-a:          ## gebze topic listesi
	$(A)/kafka-topics.sh $(BS_A) --list

topics-b:          ## ankara topic listesi
	$(B)/kafka-topics.sh $(BS_B) --list

groups-b:          ## ankara consumer group offset detayları
	$(B)/kafka-consumer-groups.sh $(BS_B) --describe --all-groups

# --- Phase 2: CDC pipeline (Debezium source + JDBC sink) ---
cdc-register:      ## İki tarafa da connector kaydet: gebze=AKTİF, ankara=PASİF (paused)
	@echo ">> gebze Connect (8084) bekleniyor..."; until curl -sf $(CONNECT_A)/ >/dev/null; do sleep 2; done
	@echo ">> ankara Connect (8085) bekleniyor..."; until curl -sf $(CONNECT_B)/ >/dev/null; do sleep 2; done
	@echo ">> eski connector'lar (varsa) temizleniyor (idempotent)"
	@curl -sf -X DELETE $(CONNECT_A)/connectors/src-customers  >/dev/null 2>&1 || true
	@curl -sf -X DELETE $(CONNECT_A)/connectors/sink-customers >/dev/null 2>&1 || true
	@curl -sf -X DELETE $(CONNECT_B)/connectors/src-customers  >/dev/null 2>&1 || true
	@curl -sf -X DELETE $(CONNECT_B)/connectors/sink-customers >/dev/null 2>&1 || true
	@sleep 2
	@echo ">> [gebze] source + sink (RUNNING)"
	@curl -sf -X POST -H "Content-Type: application/json" --data @connectors/source-postgres.json $(CONNECT_A)/connectors >/dev/null || true
	@curl -sf -X POST -H "Content-Type: application/json" --data @connectors/sink-jdbc.json     $(CONNECT_A)/connectors >/dev/null || true
	@echo ">> [ankara] yalnız SINK kaydedilip PAUSE (source standby'da çalışamaz; failover'da kaydedilir)"
	@curl -sf -X POST -H "Content-Type: application/json" --data @connectors/sink-jdbc-ankara.json $(CONNECT_B)/connectors >/dev/null || true
	@sleep 2
	@curl -sf -X PUT $(CONNECT_B)/connectors/sink-customers/pause || true
	@echo ""; echo ">> Kayıt bitti. 'make active-status' ile durumu gör."

cdc-status:        ## gebze Connect connector durumları
	@curl -sf $(CONNECT_A)/connectors?expand=status | python3 -m json.tool

cdc-test:          ## Kaynağa yeni satır ekle, hedef tabloda + mirror topic'te doğrula
	@echo ">> pg-source'a INSERT"
	@$(PSQL_S) -c "INSERT INTO inventory.customers(name,email) VALUES ('Grace Hopper','grace@example.com');"
	@echo ">> CDC akışı için 6sn bekle"; sleep 6
	@echo "==== HEDEF (pg-target.customers_replica) ===="
	@$(PSQL_T) -c "SELECT id,name,email FROM customers_replica ORDER BY id;" || echo "(tablo henüz yok? sink durumunu kontrol et: make cdc-status)"
	@echo "==== ankara'da mirror edilen CDC topic'i (gebze.dbz.inventory.customers) ===="
	@$(B)/kafka-topics.sh $(BS_B) --list | grep dbz || echo "(mirror için ~10sn daha bekle)"

cdc-delete:        ## Kaynaktan bir satır sil, hedeften de silindiğini doğrula (delete.enabled)
	@$(PSQL_S) -c "DELETE FROM inventory.customers WHERE name='Grace Hopper';"
	@sleep 6
	@$(PSQL_T) -c "SELECT id,name FROM customers_replica ORDER BY id;"

failover-check:    ## Sink consumer group offset'i iki cluster'da karşılaştır (DR hazır mı?)
	@echo "==== GEBZE: connect-sink-customers (orijinal offset) ===="
	@$(A)/kafka-consumer-groups.sh $(BS_A) --describe --group connect-sink-customers 2>/dev/null | grep -E 'TOPIC|dbz' || echo "(grup yok)"
	@echo ""
	@echo "==== ANKARA: connect-sink-customers (MM2 çevrilmiş -> failover'da buradan devam) ===="
	@$(B)/kafka-consumer-groups.sh $(BS_B) --describe --group connect-sink-customers 2>/dev/null | grep -E 'TOPIC|dbz' || echo "(grup ankara'ya sync olmamış - mm2.properties groups.exclude kontrol et)"

# --- Phase 3/4: failover (gebze->ankara) ve failback (ankara->gebze) ---
# failover GERÇEK DB promote yapar: standby (pg-source-b) yazılabilir primary olur,
# ankara Debezium senkron dbz_slot'tan (snapshot.mode=never) kaldığı LSN'den devam eder.
failover:          ## gebze DÜŞTÜ: gebze PAUSE -> standby PROMOTE -> ankara source+sink başlar, yük ankara'ya
	@echo ">> [gebze] connector'lar PAUSE"
	@curl -sf -X PUT $(CONNECT_A)/connectors/src-customers/pause  || true
	@curl -sf -X PUT $(CONNECT_A)/connectors/sink-customers/pause || true
	@echo ">> [DB] standby (pg-source-b) PROMOTE ediliyor"
	@$(PSQL_SB) -c "SELECT pg_promote(wait => true);" || true
	@echo ">> promote sonrası dbz_slot durumu (synced->normal, active olacak):"
	@$(PSQL_SB) -c "SELECT slot_name,synced,failover,active FROM pg_replication_slots;" || true
	@echo ">> yük üreteci ankara'ya (artık yazılabilir)"
	@docker compose stop loadgen-a >/dev/null 2>&1 || true
	@docker compose --profile failover up -d loadgen-b
	@echo ">> [ankara] sink RESUME + source KAYDET (snapshot.mode=never -> slot'tan devam)"
	@curl -sf -X PUT $(CONNECT_B)/connectors/sink-customers/resume || true
	@curl -sf -X POST -H "Content-Type: application/json" --data @connectors/source-postgres-ankara.json $(CONNECT_B)/connectors >/dev/null || true
	@echo ""; echo ">> Failover tamam. 'make active-status' + 'make db-status' + 'make cdc-watch'."

failback:          ## gebze GERİ GELDİ: ankara PAUSE, gebze RESUME, yük gebze'ye (DB reprovision notu)
	@echo ">> [ankara] connector'lar PAUSE + ankara source siliniyor"
	@curl -sf -X PUT $(CONNECT_B)/connectors/sink-customers/pause || true
	@curl -sf -X DELETE $(CONNECT_B)/connectors/src-customers >/dev/null 2>&1 || true
	@docker compose stop loadgen-b >/dev/null 2>&1 || true
	@echo ">> [gebze] connector'lar RESUME, yük gebze'ye"
	@docker compose up -d loadgen-a
	@curl -sf -X PUT $(CONNECT_A)/connectors/sink-customers/resume || true
	@curl -sf -X PUT $(CONNECT_A)/connectors/src-customers/resume  || true
	@echo ""
	@echo ">> NOT: failover'da pg-source-b promote edildiyse iki taraf ayrıştı (split timeline)."
	@echo ">>      DB'yi yeniden senkronlamak için (Patroni'nin otomatik yaptığı pg_rewind):"
	@echo ">>      make db-reprovision  (pg-source-b'yi sıfırdan standby olarak yeniden kurar)"

db-status:         ## Streaming replication + slot sync durumu (primary & standby)
	@echo "==== pg-source (primary) — replication clients & slots ===="
	@$(PSQL_S) -c "SELECT application_name,state,sync_state,replay_lag FROM pg_stat_replication;" 2>/dev/null || echo "(primary değil?)"
	@$(PSQL_S) -c "SELECT slot_name,slot_type,active,failover FROM pg_replication_slots;" 2>/dev/null || true
	@echo "==== pg-source-b (standby) — recovery & synced slots ===="
	@$(PSQL_SB) -c "SELECT pg_is_in_recovery() AS in_recovery;" 2>/dev/null || true
	@$(PSQL_SB) -c "SELECT slot_name,slot_type,synced,active,failover FROM pg_replication_slots;" 2>/dev/null || true
	@echo "==== veri eşitliği (primary vs standby satır sayısı) ===="
	@echo "  primary=$$($(PSQL_S) -tAc 'SELECT count(*) FROM inventory.customers' 2>/dev/null || echo -)  standby=$$($(PSQL_SB) -tAc 'SELECT count(*) FROM inventory.customers' 2>/dev/null || echo -)"

db-reprovision:    ## pg-source-b'yi sıfırdan standby olarak yeniden kur (failback sonrası DB re-sync)
	@echo ">> ankara connector'ları + loadgen-b durduruluyor"
	@curl -sf -X DELETE $(CONNECT_B)/connectors/src-customers  >/dev/null 2>&1 || true
	@curl -sf -X PUT    $(CONNECT_B)/connectors/sink-customers/pause >/dev/null 2>&1 || true
	@docker compose stop loadgen-b >/dev/null 2>&1 || true
	@echo ">> pg-source-b siliniyor ve standby olarak yeniden kuruluyor (pg_basebackup)"
	@docker compose rm -v -sf pg-source-b >/dev/null 2>&1 || true
	@docker compose up -d pg-source-b
	@echo ">> 'make db-status' ile standby tekrar senkron mu kontrol et."

active-status:     ## İki tarafın connector durumu + hangi loadgen çalışıyor
	@echo "==== GEBZE (8084) ===="
	@curl -sf "$(CONNECT_A)/connectors?expand=status" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(' ',n,'=',v['status']['connector']['state']) for n,v in d.items()]" 2>/dev/null || echo "  (ulaşılamadı)"
	@echo "==== ANKARA (8085) ===="
	@curl -sf "$(CONNECT_B)/connectors?expand=status" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(' ',n,'=',v['status']['connector']['state']) for n,v in d.items()]" 2>/dev/null || echo "  (ulaşılamadı)"
	@echo "==== loadgen ===="
	@docker compose ps --status running --format '{{.Service}}' | grep loadgen || echo "  (çalışan loadgen yok)"

cdc-watch:         ## İki hedef tablonun satır sayısını canlı izle (Ctrl+C ile çık)
	@echo "gebze pg-target vs ankara pg-target-b — satır sayıları (2sn'de bir)"
	@while true; do \
		g=$$($(PSQL_T) -tAc "SELECT count(*) FROM customers_replica" 2>/dev/null || echo "-"); \
		a=$$($(PSQL_TB) -tAc "SELECT count(*) FROM customers_replica" 2>/dev/null || echo "-"); \
		echo "$$(date +%H:%M:%S)  gebze=$$g  ankara=$$a"; sleep 2; \
	done

psql-source:       ## pg-source'a interaktif psql
	docker compose exec pg-source psql -U postgres -d sourcedb

psql-target:       ## pg-target'a interaktif psql
	docker compose exec pg-target psql -U postgres -d targetdb

help:              ## Bu yardım
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
