A := docker compose exec -T kafka-gebze /opt/kafka/bin
B := docker compose exec -T kafka-ankara /opt/kafka/bin
BS_A := --bootstrap-server localhost:19092
BS_B := --bootstrap-server localhost:19092
# Container-içi CLI INTERNAL listener'a (localhost:19092) bağlanır; HOST listener
# (9092/9094) advertised adresi container dışına işaret ettiği için içeriden kullanılmaz.

PSQL_S := docker compose exec -T pg-source-gebze psql -U postgres -d sourcedb
PSQL_T := docker compose exec -T pg-target-gebze psql -U postgres -d targetdb
PSQL_SB := docker compose exec -T pg-source-ankara psql -U postgres -d sourcedb
PSQL_TB := docker compose exec -T pg-target-ankara psql -U postgres -d targetdb
CONNECT_A := http://localhost:8084
CONNECT_B := http://localhost:8085
CONNECT := $(CONNECT_A)

# cdc-status yardımcısı: /connectors?expand=status çıktısını hizalı + renkli tabloya çevirir
export CONNSTAT_PY = import sys,json;d=json.load(sys.stdin);C={'RUNNING':'\033[32m','PAUSED':'\033[33m','FAILED':'\033[31m'};R='\033[0m';rows=sorted([n,v['status'].get('type','?'),v['status']['connector']['state'],(','.join(t['state'] for t in v['status'].get('tasks',[])) or '-')] for n,v in d.items());print('  (connector yok)') if not rows else [print('  %-24s %-7s %s%-8s%s task=%s'%(n,ty,C.get(cs,''),cs,R,ts)) for n,ty,cs,ts in rows]

.PHONY: up down clean logs ps demo verify topics-a topics-b groups-b mm2-status \
        ui lsn mm2-ankara-up mm2-gebze-up mm2-ankara-down mm2-gebze-down gebze-down loadgen-ankara-up loadgen-gebze-up promote-ankara promote-gebze dr-status _mm2-status _dr-status \
        cdc-register-gebze cdc-register-ankara cdc-prep-ankara cdc-status cdc-insert cdc-delete failover-check \
        failover failback cdc-watch db-status db-reprovision reprovision-gebze psql-source psql-target help

up:                ## Gebze + Ankara ortamını (active-standby) başlat — MM2 ve connector HARİÇ
	docker compose up -d
	@echo "Gebze + Ankara ayağa kalkıyor... 'make ps' ile health kontrol et."
	@echo "Akış: make cdc-register-gebze -> cdc-insert  (gebze CDC)"
	@echo "      make mm2-ankara-up  (gebze->ankara sync)  ->  make failover"
	@echo "      make mm2-gebze-up   (ankara->gebze sync)  ->  make failback"
	@echo "İzleme: make dr-status (canlı iki taraf) · make cdc-status · make lsn"
	@$(MAKE) --no-print-directory ui

ui:                ## Gözlem UI adreslerini yazdır (Kafka UI + Adminer)
	@echo "----------------------------------------------------------"
	@echo " Kafka UI : http://localhost:8080   (gebze + ankara cluster tek arayüz)"
	@echo " Adminer  : http://localhost:8090   (System=PostgreSQL, user/pass=postgres)"
	@echo "            hedef kayıtlar -> Server: pg-target-gebze / DB: targetdb / tablo: customers_replica"
	@echo "----------------------------------------------------------"

mm2-ankara-up:     ## MM2 gebze->ankara başlat (mm2-gebze durur — senaryo: aynı anda tek yön)
	@docker compose stop mm2-gebze >/dev/null 2>&1 || true
	docker compose --profile mm2 up -d mm2-ankara
	@echo ">> mm2-ankara başladı (gebze->ankara), mm2-gebze durduruldu. ~10sn sonra 'make dr-status'."

mm2-gebze-up:      ## MM2 ankara->gebze başlat (mm2-ankara durur — senaryo: aynı anda tek yön)
	@docker compose stop mm2-ankara >/dev/null 2>&1 || true
	docker compose --profile mm2 up -d mm2-gebze
	@echo ">> mm2-gebze başladı (ankara->gebze). Default'ta ankara.dbz.* ayrı topic'e gider, duplikasyon YOK."

mm2-ankara-down:   ## Yalnız mm2-ankara (gebze->ankara) driver'ını durdur
	docker compose stop mm2-ankara

mm2-gebze-down:    ## Yalnız mm2-gebze (ankara->gebze) driver'ını durdur
	docker compose stop mm2-gebze

gebze-down:        ## DISASTER SİM: tüm GEBZE servislerini durdur (kafka+2pg+debezium+loadgen+mm2'ler)
	docker compose stop kafka-gebze pg-source-gebze pg-target-gebze debezium-gebze loadgen-gebze mm2-gebze
	@echo ">> GEBZE DÜŞTÜ (disaster). Devral: 'make promote-ankara' + ankara connector'ları."
	@echo ">> Geri getirmek için sonra: 'make up' (durmuş gebze servislerini tekrar başlatır)."

loadgen-ankara-up: ## loadgen-ankara'yı başlat (ankara'ya sürekli INSERT — failover sonrası yük)
	docker compose --profile failover up -d loadgen-ankara
	@echo ">> loadgen-ankara başladı; ankara source'una INSERT akıyor -> CDC artık ankara'da."

loadgen-gebze-up:  ## loadgen-gebze'yi başlat (gebze'ye sürekli INSERT — failback sonrası yük)
	docker compose up -d loadgen-gebze
	@echo ">> loadgen-gebze başladı; gebze source'una INSERT akıyor."

promote-ankara:    ## Ankara standby'ı (pg-source-ankara) primary'ye promote et — failover
	@echo ">> pg-source-ankara PROMOTE ediliyor..."
	@$(PSQL_SB) -c "SELECT pg_promote(wait => true);" || true
	@$(PSQL_SB) -c "SELECT pg_is_in_recovery() AS in_recovery;" 2>/dev/null || true
	@$(PSQL_SB) -c "SELECT slot_name,synced,active,failover FROM pg_replication_slots WHERE slot_name='dbz_slot';" 2>/dev/null || true
	@echo ">> in_recovery=f ise ankara artık PRIMARY. Sonra ankara source+sink connector'larını aç."

promote-gebze:     ## PLANLI SWITCHOVER: gebze'yi PRIMARY + ankara'yı gebze'nin STANDBY'ı yap
	@test "$$($(PSQL_S) -tAc 'SELECT pg_is_in_recovery()' 2>/dev/null)" = "t" || { echo "HATA: gebze STANDBY değil (in_recovery=t olmalı; önce 'make reprovision-gebze'). Durduruldu."; exit 1; }
	@echo ">> [1/5] ankara yazması durduruluyor (kayıpsız switchover)"
	@docker compose stop loadgen-ankara >/dev/null 2>&1 || true
	@echo ">> [2/5] gebze'nin ankara'ya yetişmesi bekleniyor (~4sn)"; sleep 4
	@echo ">> [3/5] gebze PROMOTE (standby -> primary)"
	@$(PSQL_S) -c "SELECT pg_promote(wait => true);" >/dev/null 2>&1 || echo "   (promote atlandı — gebze zaten primary?)"
	@$(PSQL_S) -tAc "SELECT 'gebze in_recovery='||pg_is_in_recovery();" 2>/dev/null || true
	@echo ">> [4/5] gebze'de ankara-standby için fiziksel slot (standby_phys_slot)"
	@$(PSQL_S) -c "SELECT pg_create_physical_replication_slot('standby_phys_slot');" >/dev/null 2>&1 || echo "   (slot zaten var)"
	@echo ">> [5/5] ankara, gebze'nin STANDBY'ı olarak yeniden kuruluyor (pg_basebackup)..."
	@$(MAKE) --no-print-directory db-reprovision
	@echo ">> Switchover tamam: gebze PRIMARY, ankara STANDBY. ~30sn sonra 'make dr-status' ile doğrula."

lsn:               ## Slot sync'i LSN ile göster (rol OTOMATİK: primary current_wal / standby replay)
	@echo "══ pg-source-gebze ══"
	@$(PSQL_S)  -tAc "SELECT '  rol='||(CASE WHEN pg_is_in_recovery() THEN 'STANDBY' ELSE 'PRIMARY' END)||'  lsn='||COALESCE(pg_last_wal_replay_lsn(),pg_current_wal_lsn())" 2>/dev/null || echo "  (ulaşılamadı)"
	@$(PSQL_S)  -c "SELECT application_name,state,sent_lsn,replay_lsn FROM pg_stat_replication;" 2>/dev/null || true
	@$(PSQL_S)  -c "SELECT slot_name,restart_lsn,confirmed_flush_lsn,synced,active,failover FROM pg_replication_slots WHERE slot_name='dbz_slot';" 2>/dev/null || true
	@echo "══ pg-source-ankara ══"
	@$(PSQL_SB) -tAc "SELECT '  rol='||(CASE WHEN pg_is_in_recovery() THEN 'STANDBY' ELSE 'PRIMARY' END)||'  lsn='||COALESCE(pg_last_wal_replay_lsn(),pg_current_wal_lsn())" 2>/dev/null || echo "  (ulaşılamadı)"
	@$(PSQL_SB) -c "SELECT application_name,state,sent_lsn,replay_lsn FROM pg_stat_replication;" 2>/dev/null || true
	@$(PSQL_SB) -c "SELECT slot_name,restart_lsn,confirmed_flush_lsn,synced,active,failover FROM pg_replication_slots WHERE slot_name='dbz_slot';" 2>/dev/null || true
	@echo ">> STANDBY tarafında dbz_slot.confirmed_flush_lsn PRIMARY ile hizalıysa slot SYNC (synced=t)."

down:              ## Durdur (volume'ler kalır)
	docker compose --profile mm2 --profile failover down

clean:             ## Durdur + tüm veriyi sil
	docker compose --profile mm2 --profile failover down -v

ps:                ## Servis durumları
	docker compose ps

logs:              ## MM2 driver loglarını izle (mm2-ankara + mm2-gebze)
	docker compose logs -f mm2-ankara mm2-gebze

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
	@echo "========== ankara'da mirror edilen mesajlar (gebze.demo — Default, önekli) =========="
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

# DefaultReplicationPolicy: mirror ankara'da ÖNEKLİ -> gebze.<topic> (gebze->ankara yönü)
mm2-status:        ## MM2 topic mesaj sayısı + offset'i gebze↔ankara SÜREKLİ izle (Ctrl+C çık)
	@while true; do clear 2>/dev/null || true; $(MAKE) --no-print-directory _mm2-status; \
		echo; echo "(2sn'de bir yenilenir — Ctrl+C ile çık)"; sleep 2; done

_mm2-status:
	@echo "============= TOPIC MESAJ SAYISI (log-end) — gebze YEREL vs ankara MIRROR ============="
	@printf "  %-38s %10s %10s\n" "topic" "gebze" "ankara"
	@printf "  %-38s %10s %10s\n" "dbz.inventory.customers -> gebze.dbz.*" \
	  "$$($(A)/kafka-get-offsets.sh $(BS_A) --topic dbz.inventory.customers 2>/dev/null | awk -F: '{s+=$$3} END{print s+0}')" \
	  "$$($(B)/kafka-get-offsets.sh $(BS_B) --topic gebze.dbz.inventory.customers 2>/dev/null | awk -F: '{s+=$$3} END{print s+0}')"
	@printf "  %-38s %10s %10s\n" "demo -> gebze.demo" \
	  "$$($(A)/kafka-get-offsets.sh $(BS_A) --topic demo 2>/dev/null | awk -F: '{s+=$$3} END{print s+0}')" \
	  "$$($(B)/kafka-get-offsets.sh $(BS_B) --topic gebze.demo 2>/dev/null | awk -F: '{s+=$$3} END{print s+0}')"
	@echo ""
	@echo "============= CONSUMER GROUP OFFSET (MM2 çevirdi) ============="
	@printf "  %-38s %10s %10s\n" "group / topic" "gebze" "ankara"
	@printf "  %-38s %10s %10s\n" "connect-sink-customers" \
	  "$$($(A)/kafka-consumer-groups.sh $(BS_A) --describe --group connect-sink-customers 2>/dev/null | awk -v t=dbz.inventory.customers '$$2==t{s+=$$4} END{print s+0}')" \
	  "$$($(B)/kafka-consumer-groups.sh $(BS_B) --describe --group connect-sink-customers 2>/dev/null | awk -v t=gebze.dbz.inventory.customers '$$2==t{s+=$$4} END{print s+0}')"
	@printf "  %-38s %10s %10s\n" "demo-cg" \
	  "$$($(A)/kafka-consumer-groups.sh $(BS_A) --describe --group demo-cg 2>/dev/null | awk -v t=demo '$$2==t{s+=$$4} END{print s+0}')" \
	  "$$($(B)/kafka-consumer-groups.sh $(BS_B) --describe --group demo-cg 2>/dev/null | awk -v t=gebze.demo '$$2==t{s+=$$4} END{print s+0}')"
	@echo ""
	@echo ">> gebze(yerel) = ankara(gebze.* mirror) ise topic+offset sync tam. (ankara boşsa 'make mm2-ankara-up')"

DR_INTERVAL ?= 2
dr-status:         ## CANLI follow: her turda tarih/saat + iki taraf raporu alt alta (geçmiş silinmez, Ctrl+C çık)
	@while true; do \
		printf '\n\033[1m═══════════════ %s ═══════════════\033[0m\n' "$$(date '+%Y-%m-%d %H:%M:%S')"; \
		$(MAKE) --no-print-directory _dr-status; \
		sleep $(DR_INTERVAL); \
	done

_dr-status:
	@echo "═══════════ POSTGRES (satır + rol + LSN + slot) ═══════════"
	@$(PSQL_S)  -tAc "SELECT '  pg-source-gebze   rows='||count(*)||'  recovery='||(SELECT pg_is_in_recovery())||'  lsn='||(SELECT COALESCE(pg_last_wal_replay_lsn(),pg_current_wal_lsn()))||'  dbz_slot='||(SELECT COALESCE(confirmed_flush_lsn::text,'-')||' synced='||COALESCE(synced::text,'-') FROM pg_replication_slots WHERE slot_name='dbz_slot') FROM inventory.customers" 2>/dev/null || echo "  pg-source-gebze   (ulaşılamadı)"
	@$(PSQL_SB) -tAc "SELECT '  pg-source-ankara  rows='||count(*)||'  recovery='||(SELECT pg_is_in_recovery())||'  lsn='||(SELECT COALESCE(pg_last_wal_replay_lsn(),pg_current_wal_lsn()))||'  dbz_slot='||(SELECT COALESCE(confirmed_flush_lsn::text,'-')||' synced='||COALESCE(synced::text,'-') FROM pg_replication_slots WHERE slot_name='dbz_slot') FROM inventory.customers" 2>/dev/null || echo "  pg-source-ankara  (ulaşılamadı)"
	@$(PSQL_T)  -tAc "SELECT '  pg-target-gebze   customers_replica='||count(*) FROM customers_replica" 2>/dev/null || echo "  pg-target-gebze   (customers_replica yok)"
	@$(PSQL_TB) -tAc "SELECT '  pg-target-ankara  customers_replica='||count(*) FROM customers_replica" 2>/dev/null || echo "  pg-target-ankara  (customers_replica yok)"
	@echo ""
	@echo "═══════════ KAFKA topic mesaj sayısı (log-end) — Default: mirror ÖNEKLİ ═══════════"
	@printf "  %-40s %10s %10s\n" "topic" "gebze" "ankara"
	@printf "  %-40s %10s %10s\n" "dbz.inventory.customers (yerel)" \
	  "$$($(A)/kafka-get-offsets.sh $(BS_A) --topic dbz.inventory.customers 2>/dev/null | awk -F: '{s+=$$3} END{print s+0}')" \
	  "$$($(B)/kafka-get-offsets.sh $(BS_B) --topic dbz.inventory.customers 2>/dev/null | awk -F: '{s+=$$3} END{print s+0}')"
	@printf "  %-40s %10s %10s\n" "gebze.dbz.inventory.customers (gebze->ankara mirror)" \
	  "$$($(A)/kafka-get-offsets.sh $(BS_A) --topic gebze.dbz.inventory.customers 2>/dev/null | awk -F: '{s+=$$3} END{print s+0}')" \
	  "$$($(B)/kafka-get-offsets.sh $(BS_B) --topic gebze.dbz.inventory.customers 2>/dev/null | awk -F: '{s+=$$3} END{print s+0}')"
	@printf "  %-40s %10s %10s\n" "ankara.dbz.inventory.customers (ankara->gebze mirror)" \
	  "$$($(A)/kafka-get-offsets.sh $(BS_A) --topic ankara.dbz.inventory.customers 2>/dev/null | awk -F: '{s+=$$3} END{print s+0}')" \
	  "$$($(B)/kafka-get-offsets.sh $(BS_B) --topic ankara.dbz.inventory.customers 2>/dev/null | awk -F: '{s+=$$3} END{print s+0}')"
	@echo ""
	@echo "═══════════ OFFSET: connect-sink-customers (gebze yerel ↔ ankara mirror) ═══════════"
	@printf "  %-40s %10s %10s\n" "topic" "gebze" "ankara"
	@printf "  %-40s %10s %10s\n" "dbz.inventory.customers" \
	  "$$($(A)/kafka-consumer-groups.sh $(BS_A) --describe --group connect-sink-customers 2>/dev/null | awk -v t=dbz.inventory.customers '$$2==t{s+=$$4} END{print s+0}')" \
	  "$$($(B)/kafka-consumer-groups.sh $(BS_B) --describe --group connect-sink-customers 2>/dev/null | awk -v t=dbz.inventory.customers '$$2==t{s+=$$4} END{print s+0}')"
	@printf "  %-40s %10s %10s\n" "gebze.dbz.inventory.customers (mirror)" \
	  "$$($(A)/kafka-consumer-groups.sh $(BS_A) --describe --group connect-sink-customers 2>/dev/null | awk -v t=gebze.dbz.inventory.customers '$$2==t{s+=$$4} END{print s+0}')" \
	  "$$($(B)/kafka-consumer-groups.sh $(BS_B) --describe --group connect-sink-customers 2>/dev/null | awk -v t=gebze.dbz.inventory.customers '$$2==t{s+=$$4} END{print s+0}')"

# --- Phase 2: CDC pipeline (Debezium source + JDBC sink) ---
cdc-register-gebze: ## Gebze source + sink connector'larını RUNNING kaydet
	@echo ">> gebze Connect (8084) bekleniyor..."; until curl -sf $(CONNECT_A)/ >/dev/null; do sleep 2; done
	@echo ">> eski gebze connector'ları (varsa) temizleniyor (idempotent)"
	@curl -sf -X DELETE $(CONNECT_A)/connectors/src-customers-gebze  >/dev/null 2>&1 || true
	@curl -sf -X DELETE $(CONNECT_A)/connectors/sink-customers-gebze >/dev/null 2>&1 || true
	@sleep 2
	@echo ">> [gebze] source + sink (RUNNING)"
	@curl -sf -X POST -H "Content-Type: application/json" --data @connectors/source-postgres-gebze.json $(CONNECT_A)/connectors >/dev/null || true
	@curl -sf -X POST -H "Content-Type: application/json" --data @connectors/sink-jdbc-gebze.json    $(CONNECT_A)/connectors >/dev/null || true
	@echo ""; echo ">> gebze CDC kaydı bitti. 'make cdc-insert' ile doğrula."

cdc-prep-ankara:   ## Ankara sink'i PAUSED kaydet (failover öncesi hazırlık; MM2 offset'i sync eder)
	@echo ">> ankara Connect (8085) bekleniyor..."; until curl -sf $(CONNECT_B)/ >/dev/null; do sleep 2; done
	@curl -sf -X DELETE $(CONNECT_B)/connectors/sink-customers-ankara >/dev/null 2>&1 || true
	@sleep 2
	@echo ">> [ankara] yalnız SINK kaydedilip PAUSE (source standby'da çalışamaz; failover'da kaydedilir)"
	@curl -sf -X POST -H "Content-Type: application/json" --data @connectors/sink-jdbc-ankara.json $(CONNECT_B)/connectors >/dev/null || true
	@sleep 2
	@curl -sf -X PUT $(CONNECT_B)/connectors/sink-customers-ankara/pause || true
	@echo ""; echo ">> ankara sink PAUSED. 'make cdc-status' ile durumu gör."

cdc-register-ankara: ## Ankara source + sink connector'larını RUNNING kaydet (promote sonrası ankara devralır)
	@echo ">> ankara Connect (8085) bekleniyor..."; until curl -sf $(CONNECT_B)/ >/dev/null; do sleep 2; done
	@echo ">> [ankara] SINK kaydediliyor (RUNNING) — 'connect-sink-customers' synced offset'inden devam"
	@curl -sf -X DELETE $(CONNECT_B)/connectors/sink-customers-ankara >/dev/null 2>&1 || true
	@sleep 2
	@curl -sf -X POST -H "Content-Type: application/json" --data @connectors/sink-jdbc-ankara.json $(CONNECT_B)/connectors >/dev/null || true
	@echo ">> [ankara] SOURCE kaydediliyor (snapshot.mode=never) — senkron dbz_slot LSN'inden devam"
	@curl -sf -X DELETE $(CONNECT_B)/connectors/src-customers-ankara >/dev/null 2>&1 || true
	@sleep 2
	@curl -sf -X POST -H "Content-Type: application/json" --data @connectors/source-postgres-ankara.json $(CONNECT_B)/connectors >/dev/null || true
	@echo ""; echo ">> ankara source+sink RUNNING. 'make cdc-status' ile kontrol et."

cdc-insert:          ## Kaynağa yeni satır ekle, hedef tabloda + mirror topic'te doğrula
	@echo ">> pg-source-gebze'a INSERT"
	@$(PSQL_S) -c "INSERT INTO inventory.customers(name,email) VALUES ('Ugur Ozgen','ugur@example.com');"
	@echo ">> CDC akışı için 6sn bekle"; sleep 6
	@echo "==== HEDEF (pg-target-gebze.customers_replica) ===="
	@$(PSQL_T) -c "SELECT id,name,email FROM customers_replica where email='ugur@example.com';" || echo "(tablo henüz yok? sink durumunu kontrol et: make cdc-status)"
	@echo "==== ankara'da mirror edilen CDC topic'i (gebze.dbz.inventory.customers — Default, önekli) ===="
	@$(B)/kafka-topics.sh $(BS_B) --list | grep dbz || echo "(mirror için ~10sn daha bekle)"

cdc-delete:        ## Kaynaktan bir satır sil, hedeften de silindiğini doğrula (delete.enabled)
	@$(PSQL_S) -c "DELETE FROM inventory.customers WHERE email='ugur@example.com';"
	@sleep 6
	@$(PSQL_T) -c "SELECT id,name,email FROM customers_replica WHERE email='ugur@example.com';"

failover-check:    ## Sink consumer group offset'i iki cluster'da karşılaştır (DR hazır mı?)
	@echo "==== GEBZE: connect-sink-customers (orijinal offset) ===="
	@$(A)/kafka-consumer-groups.sh $(BS_A) --describe --group connect-sink-customers 2>/dev/null | grep -E 'TOPIC|dbz' || echo "(grup yok)"
	@echo ""
	@echo "==== ANKARA: connect-sink-customers (MM2 çevrilmiş -> failover'da buradan devam) ===="
	@$(B)/kafka-consumer-groups.sh $(BS_B) --describe --group connect-sink-customers 2>/dev/null | grep -E 'TOPIC|dbz' || echo "(grup ankara'ya sync olmamış - 'make mm2-ankara-up' + mm2-ankara.properties groups.exclude kontrol et)"

# --- Phase 3/4: failover (gebze->ankara) ve failback (ankara->gebze) ---
# failover GERÇEK DB promote yapar: standby (pg-source-ankara) yazılabilir primary olur,
# ankara Debezium senkron dbz_slot'tan (snapshot.mode=never) kaldığı LSN'den devam eder.
failover:          ## gebze DÜŞTÜ: mm2-ankara DUR -> gebze PAUSE -> standby PROMOTE -> ankara source+sink başlar
	@echo ">> [MM2] mm2-ankara durduruluyor (gebze down; mirror hata verir, ankara devralacak)"
	@docker compose stop mm2-ankara >/dev/null 2>&1 || true
	@echo ">> [gebze] connector'lar PAUSE (gebze down ise atlanır)"
	@curl -sf -X PUT $(CONNECT_A)/connectors/src-customers-gebze/pause  || true
	@curl -sf -X PUT $(CONNECT_A)/connectors/sink-customers-gebze/pause || true
	@echo ">> [DB] standby (pg-source-ankara) PROMOTE ediliyor"
	@$(PSQL_SB) -c "SELECT pg_promote(wait => true);" || true
	@echo ">> promote sonrası dbz_slot durumu (synced->normal, active olacak):"
	@$(PSQL_SB) -c "SELECT slot_name,synced,failover,active FROM pg_replication_slots;" || true
	@echo ">> yük üreteci ankara'ya (artık yazılabilir)"
	@docker compose stop loadgen-gebze >/dev/null 2>&1 || true
	@docker compose --profile failover up -d loadgen-ankara
	@echo ">> [ankara] sink RESUME + source KAYDET (snapshot.mode=never -> slot'tan devam)"
	@curl -sf -X PUT $(CONNECT_B)/connectors/sink-customers-ankara/resume || true
	@curl -sf -X POST -H "Content-Type: application/json" --data @connectors/source-postgres-ankara.json $(CONNECT_B)/connectors >/dev/null || true
	@echo ""; echo ">> Failover tamam. 'make cdc-status' + 'make db-status' + 'make cdc-watch'."

failback:          ## FAILBACK REHBERİ: ankara->gebze geri dönüş adımlarını sırayla yazdırır
	@echo "Failback (ankara PRIMARY -> tekrar gebze PRIMARY) — adımları SEN çalıştır:"
	@echo "  1) docker compose up -d pg-source-gebze pg-target-gebze   # gebze DB'leri geri (stale primary kalkar)"
	@echo "  2) make reprovision-gebze     # gebze <- ANKARA'dan basebackup ile STANDBY, senkronlanır"
	@echo "  3) make db-status             # gebze standby + slot sync doğrula"
	@echo "  4) make mm2-gebze-up          # ankara->gebze mirror (Default: ankara.dbz.* AYRI topic, duplikasyon YOK)"
	@echo "  5) make dr-status             # ankara.dbz.* mirror gebze'ye eşitleniyor mu bak"
	@echo "  6) make promote-gebze         # PLANLI SWITCHOVER: gebze PRIMARY, ankara STANDBY"
	@echo "  7) docker compose stop debezium-ankara   # ankara CDC dur (down DEĞİL, stop!)"
	@echo "  8) make mm2-gebze-down        # ankara->gebze mirror artık gereksiz"
	@echo "  9) docker compose up -d debezium-gebze && make cdc-register-gebze   # CDC gebze'ye döndü (synced offset'ten)"
	@echo " 10) make mm2-ankara-up         # tekrar gebze->ankara backup (normal DR duruşu)"
	@echo ">> Default policy: mirror'lar ayrı prefixed topic'lerde (gebze.dbz.* / ankara.dbz.*) -> topic reset GEREKMEZ."

db-status:         ## Streaming replication + slot sync (rol OTOMATİK: hangisi primary/standby olursa)
	@echo "═══════════ pg-source-gebze ═══════════"
	@$(PSQL_S)  -tAc "SELECT '  rol = '||(CASE WHEN pg_is_in_recovery() THEN 'STANDBY (in_recovery=t)' ELSE 'PRIMARY (in_recovery=f)' END)||'   lsn = '||COALESCE(pg_last_wal_replay_lsn(),pg_current_wal_lsn())" 2>/dev/null || echo "  (ulaşılamadı)"
	@$(PSQL_S)  -c "SELECT application_name,state,sync_state,replay_lag FROM pg_stat_replication;" 2>/dev/null || true
	@$(PSQL_S)  -c "SELECT slot_name,slot_type,active,synced,failover,confirmed_flush_lsn FROM pg_replication_slots;" 2>/dev/null || true
	@echo "═══════════ pg-source-ankara ═══════════"
	@$(PSQL_SB) -tAc "SELECT '  rol = '||(CASE WHEN pg_is_in_recovery() THEN 'STANDBY (in_recovery=t)' ELSE 'PRIMARY (in_recovery=f)' END)||'   lsn = '||COALESCE(pg_last_wal_replay_lsn(),pg_current_wal_lsn())" 2>/dev/null || echo "  (ulaşılamadı)"
	@$(PSQL_SB) -c "SELECT application_name,state,sync_state,replay_lag FROM pg_stat_replication;" 2>/dev/null || true
	@$(PSQL_SB) -c "SELECT slot_name,slot_type,active,synced,failover,confirmed_flush_lsn FROM pg_replication_slots;" 2>/dev/null || true
	@echo "═══════════ veri eşitliği (inventory.customers satır sayısı) ═══════════"
	@echo "  gebze=$$($(PSQL_S) -tAc 'SELECT count(*) FROM inventory.customers' 2>/dev/null || echo -)   ankara=$$($(PSQL_SB) -tAc 'SELECT count(*) FROM inventory.customers' 2>/dev/null || echo -)"
	@echo "  (streaming'de olan taraf 'pg_stat_replication'da downstream'i gösterir; standby'da o tablo boştur)"

db-reprovision:    ## pg-source-ankara'yi sıfırdan standby olarak yeniden kur (failback sonrası DB re-sync)
	@echo ">> ankara connector'ları + loadgen-ankara durduruluyor"
	@curl -sf -X DELETE $(CONNECT_B)/connectors/src-customers-ankara  >/dev/null 2>&1 || true
	@curl -sf -X PUT    $(CONNECT_B)/connectors/sink-customers-ankara/pause >/dev/null 2>&1 || true
	@docker compose stop loadgen-ankara >/dev/null 2>&1 || true
	@echo ">> pg-source-ankara siliniyor ve standby olarak yeniden kuruluyor (pg_basebackup)"
	@docker compose rm -v -sf pg-source-ankara >/dev/null 2>&1 || true
	@docker compose up -d pg-source-ankara
	@echo ">> 'make db-status' ile standby tekrar senkron mu kontrol et."

reprovision-gebze: ## Gebze'yi ANKARA'nın standby'ı yap (production-doğru failback: geride kalan gebze <- ileri ankara)
	@test "$$($(PSQL_SB) -tAc 'SELECT pg_is_in_recovery()' 2>/dev/null)" = "f" || { echo "HATA: ankara PRIMARY değil (önce 'make promote-ankara'). Durduruldu."; exit 1; }
	@echo ">> [1/4] ankara'da gebze için fiziksel slot (gebze_phys_slot) oluşturuluyor"
	@$(PSQL_SB) -c "SELECT pg_create_physical_replication_slot('gebze_phys_slot');" >/dev/null 2>&1 || echo "   (slot zaten var)"
	@echo ">> [2/4] gebze servisleri durduruluyor (loadgen + debezium + pg)"
	@docker compose stop loadgen-gebze debezium-gebze pg-source-gebze >/dev/null 2>&1 || true
	@echo ">> [3/4] gebze, ankara'dan pg_basebackup ile standby olarak kuruluyor..."
	@docker compose run --rm --no-deps --entrypoint bash pg-source-gebze /reprovision-gebze.sh
	@echo ">> [4/4] gebze standby olarak başlatılıyor"
	@docker compose up -d pg-source-gebze
	@echo ">> ~20sn sonra 'make lsn': gebze in_recovery=t (ankara standby'ı) + ankara->gebze streaming."

cdc-status:     ## İki tarafın connector + task durumu (renkli) + aktif loadgen
	@printf '\033[1m──── GEBZE  (localhost:8084) ────────────\033[0m\n'
	@curl -sf "$(CONNECT_A)/connectors?expand=status" 2>/dev/null | python3 -c "$$CONNSTAT_PY" 2>/dev/null || echo "  (ulaşılamadı — gebze Connect kapalı?)"
	@printf '\033[1m──── ANKARA (localhost:8085) ────────────\033[0m\n'
	@curl -sf "$(CONNECT_B)/connectors?expand=status" 2>/dev/null | python3 -c "$$CONNSTAT_PY" 2>/dev/null || echo "  (connector yok / ulaşılamadı)"
	@printf '\033[1m──── loadgen ────────────────────────────\033[0m\n'
	@lg=$$(docker compose ps --status running --format '{{.Service}}' 2>/dev/null | grep loadgen); echo "  $${lg:-(çalışan loadgen yok)}"

cdc-watch:         ## İki hedef tablonun satır sayısını canlı izle (Ctrl+C ile çık)
	@echo "gebze pg-target-gebze vs ankara pg-target-ankara — satır sayıları (2sn'de bir)"
	@while true; do \
		g=$$($(PSQL_T) -tAc "SELECT count(*) FROM customers_replica" 2>/dev/null || echo "-"); \
		a=$$($(PSQL_TB) -tAc "SELECT count(*) FROM customers_replica" 2>/dev/null || echo "-"); \
		echo "$$(date +%H:%M:%S)  gebze=$$g  ankara=$$a"; sleep 2; \
	done

psql-source:       ## pg-source-gebze'a interaktif psql
	docker compose exec pg-source-gebze psql -U postgres -d sourcedb

psql-target:       ## pg-target-gebze'a interaktif psql
	docker compose exec pg-target-gebze psql -U postgres -d targetdb

help:              ## Bu yardım
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
