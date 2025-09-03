README — Coletor Festo DT (OPC UA + MPU6050 via ESP32)

Este backend coleta dois fluxos de dados em paralelo e grava no MySQL (produção) ou CSV (teste):

OPC UA → opc_samples (booleans dos nodes do CLP, conforme nodes.csv)

MPU6050 via ESP32 (Serial) → mpu_samples (ax/ay/az em g dos sensores MPUA1 e MPUA2)

Suporta três modos de execução individuais e um modo simultâneo:
SIMULATE, OPCUA, SERIAL_MPU, DUAL (OPC+MPU juntos).

1) Estrutura do projeto
backend/
├─ app.py                   # coletor principal (SIMULATE | OPCUA | SERIAL_MPU | DUAL)
├─ nodes.csv                # mapeamento dos sinais OPC UA (nome,nodeid,datatype)
├─ requirements.txt
├─ .env                     # configurações (produção ou local)
├─ bank_opc.csv             # (opcional) CSV de apoio para OPC
└─ bank_mpu.csv             # (opcional) CSV de apoio para MPU

2) Requisitos

Python 3.11+

MySQL 8+ (ou compatível)

Porta Serial para o ESP32 (apenas se usar SERIAL_MPU ou DUAL com ESP conectado)

Instale as dependências:

python -m venv .venv
# Windows
. .venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate

pip install -r requirements.txt


requirements.txt:

python-dotenv==1.0.1
opcua==0.98.13
mysql-connector-python==9.0.0
pyserial==3.5

3) Configuração (.env)
Produção — coleta simultânea (OPC + MPU) → MySQL

Crie/edite .env:

# Rodar os DOIS ao mesmo tempo
DATA_MODE=DUAL
SINK_MODE=MYSQL

# OPC UA
OPCUA_ENDPOINT=opc.tcp://192.168.0.40:4840
POLL_INTERVAL=1
OPCUA_NODEIDS_CSV=nodes.csv
# OPCUA_USER=   # (opcional; seu flow Node-RED não usava)
# OPCUA_PASS=

# Serial (ESP32)
SERIAL_PORT=COM3         # Windows | Linux: /dev/ttyUSB0 | macOS: /dev/tty.usbserial-xxxx
SERIAL_BAUD=115200
SERIAL_TIMEOUT=0.2

# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DB=festo_dt
MYSQL_USER=SEU_USUARIO
MYSQL_PASS=SUA_SENHA

Teste rápido — CSV (sem banco)
DATA_MODE=DUAL
SINK_MODE=CSV
OPCUA_ENDPOINT=opc.tcp://192.168.0.40:4840
POLL_INTERVAL=1
OPCUA_NODEIDS_CSV=nodes.csv
SERIAL_PORT=COM3
SERIAL_BAUD=115200
SERIAL_TIMEOUT=0.2
# destinos CSV de apoio (default):
# CSV_OPC_PATH=bank_opc.csv
# CSV_MPU_PATH=bank_mpu.csv


Para rodar apenas um fluxo, troque DATA_MODE para OPCUA, SERIAL_MPU ou SIMULATE.

4) DDL do banco (MySQL)

Execute uma vez:

CREATE DATABASE IF NOT EXISTS festo_dt
  DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE festo_dt;

-- 1) Leituras OPC UA (booleans)
CREATE TABLE IF NOT EXISTS opc_samples (
  id          BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  ts_utc      DATETIME(6) NOT NULL,         -- timestamp UTC da leitura
  name        VARCHAR(128) NOT NULL,        -- ex.: Avancado_1S2, V1_12, INICIA
  value_bool  TINYINT(1) NULL,              -- 0/1 ou NULL se falhou
  INDEX idx_opc_name_ts (name, ts_utc),
  INDEX idx_opc_ts (ts_utc)
);

-- 2) Leituras MPU6050 (vibração)
CREATE TABLE IF NOT EXISTS mpu_samples (
  id       BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  ts_utc   DATETIME(6) NOT NULL,      -- timestamp UTC
  mpu_id   TINYINT UNSIGNED NOT NULL, -- 1 = MPUA1, 2 = MPUA2
  ax_g     DOUBLE NOT NULL,           -- aceleração X em g
  ay_g     DOUBLE NOT NULL,           -- aceleração Y em g
  az_g     DOUBLE NOT NULL,           -- aceleração Z em g
  gx_dps   DOUBLE NULL,               -- giroscópio X °/s (opcional)
  gy_dps   DOUBLE NULL,               -- giroscópio Y °/s (opcional)
  gz_dps   DOUBLE NULL,               -- giroscópio Z °/s (opcional)
  temp_c   DOUBLE NULL,               -- temperatura chip (opcional)
  INDEX idx_mpu_id_ts (mpu_id, ts_utc),
  INDEX idx_mpu_ts (ts_utc)
);

5) nodes.csv (mapa de sinais OPC UA)

Cabeçalho obrigatório: name,nodeid,datatype. Exemplo:

name,nodeid,datatype
Avancado_1S2,ns=4;s=|var|CECC-LK.Application.IoConfig_Globals_Mapping.Avancado_1S2,Boolean
Recuado_1S1,ns=4;s=|var|CECC-LK.Application.IoConfig_Globals_Mapping.Recuado_1S1,Boolean
Avancado_2S2,ns=4;s=|var|CECC-LK.Application.IoConfig_Globals_Mapping.Avancado_2S2,Boolean
Recuado_2S1,ns=4;s=|var|CECC-LK.Application.IoConfig_Globals_Mapping.Recuado_2S1,Boolean
V1_12,ns=4;s=|var|CECC-LK.Application.IoConfig_Globals_Mapping.V1_12,Boolean
V1_14,ns=4;s=|var|CECC-LK.Application.IoConfig_Globals_Mapping.V1_14,Boolean
V2_12,ns=4;s=|var|CECC-LK.Application.IoConfig_Globals_Mapping.V2_12,Boolean
V2_14,ns=4;s=|var|CECC-LK.Application.IoConfig_Globals_Mapping.V2_14,Boolean
INICIA,ns=4;s=|var|CECC-LK.Application.IoConfig_Globals_Mapping.Inicia,Boolean
PARA,ns=4;s=|var|CECC-LK.Application.IoConfig_Globals_Mapping.Para,Boolean

6) Formato dos dados do ESP32 (MPU)

O ESP envia uma linha JSON por amostra (UTF-8), por sensor:

{"id":"MPUA1","ax":0.012,"ay":-0.004,"az":0.998}
{"id":"MPUA2","ax":0.020,"ay":-0.002,"az":1.003}


Também aceitamos "sensor" no lugar de "id". Campos opcionais (se existirem, serão gravados): gx_dps, gy_dps, gz_dps, temp_c.

7) Como rodar
Produção (DUAL → MySQL)
python app.py


Verifique inserções:

SELECT * FROM festo_dt.opc_samples ORDER BY ts_utc DESC LIMIT 10;
SELECT * FROM festo_dt.mpu_samples ORDER BY ts_utc DESC LIMIT 10;

CSV (apoio/diagnóstico)

No .env: SINK_MODE=CSV
Arquivos crescem em tempo real: bank_opc.csv e bank_mpu.csv.

8) O que cada parte do código faz (resumo destrinchado)

Config & Utils

Lê .env, monta constantes e conversores de timestamp (now_utc_iso, iso_to_mysql_dt6).

Leitura nodes.csv

load_nodes: carrega mapeamento name → nodeid,datatype para ler no OPC UA.

Fontes de dados

Simulator: alterna booleans (teste).

OpcUaReader: conecta no endpoint, get_node(nodeid).get_value() para cada name do nodes.csv.

SerialMpuReader: abre porta serial, lê uma linha JSON, valida id/sensor (MPUA1/MPUA2) e ax/ay/az; normaliza opcionais.

Sinks (destinos)

CSV (CsvOpcSink, CsvMpuSink): arquivos bank_opc.csv (wide) e bank_mpu.csv (normalizado).

MySQL (MySqlOpcSink, MySqlMpuSink): cria tabelas se não existirem e insere (opc_samples, mpu_samples).

Loops

opc_loop: a cada POLL_INTERVAL, lê todos os sinais OPC e grava (MySQL/CSV).

mpu_loop: leitura contínua da serial (linha por linha) e grava (MySQL/CSV).

Execução

DATA_MODE=DUAL: roda opc_loop e mpu_loop em threads simultâneas; controladas por Ctrl+C/SIGTERM.

Outros modos rodam apenas um loop.