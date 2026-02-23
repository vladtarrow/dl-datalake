# DL-Datalake REST API Documentation

Полная документация по REST API для работы с dl-datalake.

## Содержание

1. [Обзор](#обзор)
2. [Технические детали](#технические-детали)
3. [Основной API](#основной-api)
4. [UI API](#ui-api)
5. [Примеры использования](#примеры-использования)
6. [Коды ошибок](#коды-ошибок)

---

## Обзор

В проекте доступны два API сервера:

1. **Основной API** (`api_server.py`) - базовый REST API
   - Базовый URL: `http://localhost:8000`
   - Запуск: `uvicorn dl_datalake.client.api_server:app --reload`

2. **UI API** (`dl-datalake-ui/backend`) - расширенный API с дополнительными возможностями
   - Базовый URL: `http://localhost:8000`
   - Префикс: `/api/v1`
   - Запуск: `uvicorn main:app --reload --port 8000`

---

## Технические детали

### Лимиты и Rate Limiting

Система автоматически обрабатывает ограничения бирж (Rate Limits):
*   **Ошибка 429 (Too Many Requests)**: При получении ошибки `ccxt.DDoSProtection` система делает паузу (обычно 30 секунд) и повторяет запрос.
*   **Retry Logic**: Встроена логика повторных попыток (до 3-х раз) для этапа пробинга даты листинга и основного цикла загрузки.
*   **IP Bans (418)**: Постоянное игнорирование Retry-After может привести к бану IP. Система старается этого избегать, соблюдая рекомендации биржи.

### Формат данных: Parquet

Все рыночные данные хранятся в формате **Apache Parquet**. Это бинарный колоночный формат, который обеспечивает:
*   **Сжатие**: Данные занимают в 5-10 раз меньше места, чем CSV.
*   **Скорость**: Чтение только нужных колонок (например, только `close` для индикаторов) без загрузки всего файла.
*   **Метаданные**: В каждом файле хранятся Min/Max значения колонок, что позволяет пропускать ненужные блоки данных при фильтрации.

**Внутренняя структура:**
*   **Header**: Магическое слово `PAR1`.
*   **Row Groups**: Данные разбиты на группы строк (для параллельной обработки).
*   **Column Chunks**: Колонки хранятся физически отдельно внутри группы.
*   **Footer**: Содержит схему и статистику для оптимизации запросов.

---

## Основной API

Базовые endpoints для работы с данными.

### 1. Health Check

**GET** `/health`

Проверка работоспособности API.

**Пример запроса:**
```bash
curl "http://localhost:8000/health"
```

**Пример ответа:**
```json
{
  "status": "ok"
}
```

---

### 2. Список записей в манифесте

**GET** `/list`

Получить список всех записей в манифесте с фильтрацией.

**Параметры запроса:**
- `symbol` (optional): Фильтр по символу (BTCUSDT, ETHUSDT, etc.)
- `data_type` (optional): Фильтр по типу данных (raw, ticks, agg, alt, или имя feature_set)
- `exchange` (optional): Фильтр по бирже (BINANCE, BYBIT, etc.)
- `market` (optional): Фильтр по типу рынка (SPOT, FUTURES, etc.)

**Пример запроса:**
```bash
# Все записи для BTCUSDT
curl "http://localhost:8000/list?symbol=BTCUSDT"

# Только raw данные для SPOT рынка
curl "http://localhost:8000/list?symbol=BTCUSDT&data_type=raw&market=SPOT"

# Все записи с биржи BINANCE
curl "http://localhost:8000/list?exchange=BINANCE"

# Все записи для FUTURES рынка
curl "http://localhost:8000/list?market=FUTURES"
```

**Пример ответа:**
```json
[
  {
    "id": 1,
    "symbol": "BTCUSDT",
    "exchange": "BINANCE",
    "market": "SPOT",
    "path": "data/BINANCE/SPOT/BTCUSDT/raw/1m/2024/01/15/BTCUSDT_1m_20240115.parquet",
    "type": "raw"
  },
  {
    "id": 2,
    "symbol": "BTCUSDT",
    "exchange": "BINANCE",
    "market": "SPOT",
    "path": "data/features/rsi_indicators/1.0.0/features.parquet",
    "type": "rsi_indicators"
  }
]
```

---

### 3. Чтение данных

**GET** `/read`

Прочитать рыночные данные за указанный период.

**Параметры запроса:**
- `exchange` (required): Биржа (BINANCE, BYBIT, etc.)
- `symbol` (required): Торговый символ (BTCUSDT, ETHUSDT, etc.)
- `start` (required): Начальная дата в формате ISO (YYYY-MM-DD)
- `end` (required): Конечная дата в формате ISO (YYYY-MM-DD)
- `data_type` (optional): Тип данных (raw, agg, ticks, alt). По умолчанию "raw"

**Пример запроса:**
```bash
curl "http://localhost:8000/read?exchange=BINANCE&symbol=BTCUSDT&start=2024-01-01&end=2024-01-31&data_type=raw"
```

**Пример ответа:**
```json
[
  {
    "ts": 1704067200000,
    "open": 42000.0,
    "high": 42500.0,
    "low": 41800.0,
    "close": 42300.0,
    "volume": 1234.56
  },
  {
    "ts": 1704067260000,
    "open": 42300.0,
    "high": 42400.0,
    "low": 42200.0,
    "close": 42350.0,
    "volume": 987.65
  }
]
```

---

## Feature Store API (Основной API)

### 4. Список всех фич

**GET** `/features`

Получить список всех фич с версиями.

**Параметры запроса:**
- `exchange` (optional): Фильтр по бирже
- `symbol` (optional): Фильтр по символу
- `market` (optional): Фильтр по типу рынка
- `feature_set` (optional): Фильтр по имени набора фич
- `version` (optional): Фильтр по версии

**Пример запроса:**
```bash
# Все фичи для BTCUSDT
curl "http://localhost:8000/features?symbol=BTCUSDT"

# Фичи только для SPOT рынка
curl "http://localhost:8000/features?symbol=BTCUSDT&market=SPOT"
```

**Пример ответа:**
```json
[
  {
    "id": 123,
    "exchange": "BINANCE",
    "symbol": "BTCUSDT",
    "market": "SPOT",
    "feature_set": "rsi_indicators",
    "version": "1.0.0",
    "path": "data/features/rsi_indicators/1.0.0/features.parquet",
    "checksum": "abc123...",
    "created_at": "2024-01-15T10:30:00"
  }
]
```

---

### 5. Список feature sets с версиями

**GET** `/features/sets`

Получить сгруппированный список всех feature sets с доступными версиями.

**Параметры запроса:**
- `exchange` (optional): Фильтр по бирже
- `symbol` (optional): Фильтр по символу
- `market` (optional): Фильтр по типу рынка

**Пример запроса:**
```bash
# Все feature sets для BTCUSDT
curl "http://localhost:8000/features/sets?symbol=BTCUSDT"

# Feature sets только для SPOT рынка
curl "http://localhost:8000/features/sets?symbol=BTCUSDT&market=SPOT"
```

**Пример ответа:**
```json
{
  "feature_sets": [
    {
      "name": "rsi_indicators",
      "exchange": "BINANCE",
      "symbol": "BTCUSDT",
      "market": "SPOT",
      "versions": ["2.0.0", "1.0.0"]
    },
    {
      "name": "backtest_reports",
      "exchange": "BINANCE",
      "symbol": "BTCUSDT",
      "market": "SPOT",
      "versions": ["2024.01.15", "2024.01.10"]
    }
  ]
}
```

---

### 6. Загрузка фичи

**POST** `/features/upload`

Загрузить файл фичи в Feature Store.

**Параметры запроса (multipart/form-data):**
- `file` (required): Файл для загрузки (любой формат)
- `exchange` (required): Биржа
- `market` (required): Тип рынка (SPOT, FUTURES, etc.)
- `symbol` (required): Торговый символ
- `feature_set` (required): Имя набора фич
- `version` (optional): Версия (по умолчанию "1.0.0")

**Пример запроса (curl):**
```bash
curl -X POST "http://localhost:8000/features/upload" \
  -F "file=@./my_features.parquet" \
  -F "exchange=BINANCE" \
  -F "market=SPOT" \
  -F "symbol=BTCUSDT" \
  -F "feature_set=rsi_indicators" \
  -F "version=1.0.0"
```

**Пример запроса (Python):**
```python
import requests

url = "http://localhost:8000/features/upload"
files = {"file": open("my_features.parquet", "rb")}
data = {
    "exchange": "BINANCE",
    "market": "SPOT",
    "symbol": "BTCUSDT",
    "feature_set": "rsi_indicators",
    "version": "1.0.0"
}

response = requests.post(url, files=files, data=data)
print(response.json())
```

**Пример ответа:**
```json
{
  "status": "success",
  "version": "1.0.0",
  "message": "Feature rsi_indicators v1.0.0 uploaded successfully"
}
```

---

### 7. Получение метаданных фичи

**GET** `/features/{feature_id}`

Получить метаданные конкретной фичи по ID.

**Параметры пути:**
- `feature_id` (required): ID записи в манифесте

**Пример запроса:**
```bash
curl "http://localhost:8000/features/123"
```

**Пример ответа:**
```json
{
  "id": 123,
  "exchange": "BINANCE",
  "symbol": "BTCUSDT",
  "market": "SPOT",
  "feature_set": "rsi_indicators",
  "version": "1.0.0",
  "file_path": "data/features/rsi_indicators/1.0.0/features.parquet",
  "file_size_bytes": 1024000,
  "checksum": "abc123...",
  "last_modified": "2024-01-15T10:30:00",
  "created_at": "2024-01-15T10:30:00",
  "metadata": {
    "feature_set": "rsi_indicators"
  }
}
```

---

### 8. Скачивание фичи

**GET** `/features/{feature_id}/download`

Скачать файл фичи по ID.

**Параметры пути:**
- `feature_id` (required): ID записи в манифесте

**Пример запроса:**
```bash
curl "http://localhost:8000/features/123/download" -o downloaded_feature.parquet
```

**Ответ:** Бинарный файл с заголовками для скачивания.

---

### 9. Удаление фичи

**DELETE** `/features/{feature_id}`

Удалить фичу по ID (удаляет файл и запись в манифесте).

**Параметры пути:**
- `feature_id` (required): ID записи в манифесте

**Пример запроса:**
```bash
curl -X DELETE "http://localhost:8000/features/123"
```

**Пример ответа:**
```json
{
  "status": "success",
  "message": "Feature rsi_indicators v1.0.0 deleted"
}
```

---

## UI API

Расширенный API с дополнительными возможностями. Все endpoints имеют префикс `/api/v1`.

### 11. Список датасетов

**GET** `/api/v1/datasets`

Получить список всех датасетов (рыночные данные + фичи) с пагинацией и фильтрацией.

**Параметры запроса:**
- `exchange` (optional): ID биржи (например, `BINANCE`, `BYBIT`). **Примечание**: обычно в ВЕРХНЕМ РЕГИСТРЕ.
- `symbol` (optional): Торговый символ (например, `BTCUSDT`). **Примечание**: обычно в ВЕРХНЕМ РЕГИСТРЕ.
- `market` (optional): Тип рынка (`SPOT`, `FUTURES`, `LINEAR`, `SWAP`).
- `data_type` (optional): Тип данных (`raw`, `ticks`, `agg`, `alt`) или имя набора фич.
- `limit` (optional): Сколько записей вернуть за один запрос. По умолчанию `20`.
- `offset` (optional): Смещение для пагинации (пропускает первые N записей). По умолчанию `0`.

> [!TIP]
> **Регистр имеет значение**: В базе данных значения `exchange`, `symbol` и `market` хранятся в верхнем регистре. Если поиск ничего не выдает, попробуйте `exchange=BINANCE` вместо `exchange=binance`.

**Примеры запросов:**
```bash
# Найти все RAW данные для BTC на Binance
curl "http://localhost:8000/api/v1/datasets?exchange=BINANCE&symbol=BTCUSDT&data_type=raw"

# Показать следующую страницу (записи с 21 по 40)
curl "http://localhost:8000/api/v1/datasets?exchange=BINANCE&offset=20&limit=20"

# Получить сразу 100 записей
curl "http://localhost:8000/api/v1/datasets?exchange=BINANCE&limit=100"
```

**Пример запроса:**
```bash
# Все датасеты для BTCUSDT
curl "http://localhost:8000/api/v1/datasets?symbol=BTCUSDT&limit=50&offset=0"

# Только SPOT датасеты
curl "http://localhost:8000/api/v1/datasets?symbol=BTCUSDT&market=SPOT"
```

**Пример ответа:**
```json
{
  "datasets": [
    {
      "id": "1",
      "exchange": "BINANCE",
      "symbol": "BTCUSDT",
      "market": "SPOT",
      "timeframe": "1m",
      "data_type": "raw",
      "file_path": "/path/to/file.parquet",
      "file_size_bytes": 1024000,
      "last_modified": "2024-01-15T10:30:00",
      "time_from": "2024-01-01T00:00:00",
      "time_to": "2024-01-31T23:59:59"
    }
  ],
  "total": 1
}
```

---

### 11. Предпросмотр датасета

**GET** `/api/v1/datasets/{dataset_id}/preview`

Получить предпросмотр данных датасета (первые N строк).

**Параметры пути:**
- `dataset_id` (required): ID датасета

**Параметры запроса:**
- `limit` (optional): Количество строк для предпросмотра (по умолчанию 100)
- `offset` (optional): Смещение (по умолчанию 0)

**Пример запроса:**
```bash
curl "http://localhost:8000/api/v1/datasets/1/preview?limit=50"
```

**Пример ответа:**
```json
{
  "columns": ["ts", "open", "high", "low", "close", "volume"],
  "rows": [
    {
      "ts": 1704067200000,
      "open": 42000.0,
      "high": 42500.0,
      "low": 41800.0,
      "close": 42300.0,
      "volume": 1234.56
    }
  ],
  "total_rows": 44640,
  "metadata": {
    "timeframe": "1m"
  }
}
```

---

### 12. Экспорт датасета

**GET** `/api/v1/datasets/{dataset_id}/export`

Экспортировать конкретный сегмент данных в формат CSV (совместимый с торговыми терминалами).

**Параметры пути:**
- `dataset_id` (required): ID датасета

**Пример запроса:**
```bash
curl "http://localhost:8000/api/v1/datasets/1/export"
```

**Пример ответа:**
```json
{
  "status": "success",
  "filename": "dl_BTCUSDT_BINANCE_SPOT.csv.txt",
  "path": "/path/to/trading-research/export/BINANCE/SPOT/dl_BTCUSDT_BINANCE_SPOT.csv.txt"
}
```

---

### 13. Удаление датасета

**DELETE** `/api/v1/datasets/{dataset_id}`

Удалить датасет по ID.

**Параметры пути:**
- `dataset_id` (required): ID датасета

**Пример запроса:**
```bash
curl -X DELETE "http://localhost:8000/api/v1/datasets/1"
```

**Пример ответа:**
```json
{
  "status": "success"
}
```

---

## Ingestion API (UI API)

### 14. Статус загрузок

**GET** `/api/v1/ingest/status`

Получить статус всех активных загрузок данных.

**Пример запроса:**
```bash
curl "http://localhost:8000/api/v1/ingest/status"
```

**Пример ответа:**
```json
{
  "binance:spot:BTCUSDT:raw": {
    "status": "running",
    "exchange": "binance",
    "market": "spot",
    "symbol": "BTCUSDT",
    "data_type": "raw",
    "message": "Fetched 5,000 candles...",
    "start_time": "2024-01-15T10:30:00Z"
  }
}
```

---

### 15. Загрузка истории с биржи

**POST** `/api/v1/ingest/download`

Запустить загрузку исторических данных с биржи.

**Тело запроса (JSON):**
```json
{
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "market": "spot",
  "timeframe": "1m",
  "data_type": "raw",
  "start_date": "2024-01-01",
  "full_history": false
}
```

**Параметры:**
- `exchange` (required): Биржа (binance, bybit, etc.)
- `symbol` (required): Торговый символ
- `market` (optional): Тип рынка (spot, future, etc.)
- `timeframe` (optional): Таймфрейм (по умолчанию "1m")
- `data_type` (optional): Тип данных (raw, funding, both)
- `start_date` (optional): Дата начала (YYYY-MM-DD)
- `full_history` (optional): Если true, игнорирует `start_date` и качает всё с даты листинга.

**Пример запроса:**
```bash
curl -X POST "http://localhost:8000/api/v1/ingest/download" \
  -H "Content-Type: application/json" \
  -d '{
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "market": "spot",
    "start_date": "2024-01-01"
  }'
```

**Пример ответа:**
```json
{
  "task_id": "dl_BTCUSDT",
  "status": "pending",
  "message": "Queued download for BTCUSDT from binance"
}
```

---

### 16. Массовая загрузка

**POST** `/api/v1/ingest/bulk-download`

Запустить загрузку для нескольких символов одновременно.

**Тело запроса (JSON):**
```json
{
  "exchange": "binance",
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "market": "spot",
  "timeframe": "1m",
  "data_type": "raw",
  "start_date": "2024-01-01"
}
```

**Пример ответа:**
```json
{
  "task_id": "bulk_dl",
  "status": "pending",
  "message": "Queued 2 downloads from binance"
}
```

---

### 17. Список бирж

**GET** `/api/v1/ingest/exchanges`

Получить список всех доступных бирж (из CCXT).

**Пример ответа:**
```json
{
  "exchanges": [
    { "id": "binance", "name": "Binance" },
    { "id": "bybit", "name": "Bybit" }
  ]
}
```

---

### 18. Список рынков биржи

**GET** `/api/v1/ingest/exchanges/{exchange_id}/markets`

Получить список доступных типов рынков для биржи.

---

### 19. Список символов биржи

**GET** `/api/v1/ingest/exchanges/{exchange_id}/symbols`

Получить список всех активных символов на бирже для конкретного типа рынка.

**Параметры запроса:**
- `market`: (optional) Тип рынка (по умолчанию "spot")

---

### 20. Загрузка локального файла

**POST** `/api/v1/ingest/file`

Загрузить данные из локального CSV файла в систему.

---

### 21. Агрегированный экспорт

**GET** `/api/v1/export/{exchange}/{symbol}`

Найти все фрагменты данных (1m raw) для тикера, склеить их в правильном порядке и экспортировать в один большой CSV файл.

**Параметры пути:**
- `exchange`: ID биржи
- `symbol`: Символ (если содержит `/`, замените на `_` или передайте как есть, API обработает)

**Параметры запроса:**
- `market`: (optional) Тип рынка

**Пример запроса:**
```bash
curl "http://localhost:8000/api/v1/export/binance/BTCUSDT?market=spot"
```

**Пример ответа:**
```json
{
  "status": "success",
  "filename": "dl_BTCUSDT_BINANCE_SPOT.csv.txt",
  "path": "/path/to/trading-research/export/BINANCE/SPOT/dl_BTCUSDT_BINANCE_SPOT.csv.txt",
  "rows_exported": 525600
}
```

---

### 22. Удаление истории

**DELETE** `/api/v1/ingest/exchanges/{exchange_id}/markets/{market_id}/history`

Полное удаление данных и записей в манифесте для конкретного символа.

**Параметры пути:**
- `exchange_id`: ID биржи
- `market_id`: Тип рынка

**Параметры запроса:**
- `symbol`: (required) Торговый символ
- `data_type`: (optional) Тип данных (raw, funding, etc.)

**Пример запроса:**
```bash
curl -X DELETE "http://localhost:8000/api/v1/ingest/exchanges/binance/markets/spot/history?symbol=BTCUSDT"
```

**Пример ответа:**
```json
{
  "status": "success",
  "deleted_entries": 12,
  "deleted_files": 12,
  "message": "Deleted 12 entries and 12 files for BTCUSDT"
}
```

---

## Feature Store API (UI API)

Все endpoints для работы с фичами также доступны в UI API с префиксом `/api/v1/features`:

- `GET /api/v1/features` - Список всех фич (с пагинацией)
- `GET /api/v1/features/sets` - Список feature sets с версиями
- `POST /api/v1/features/upload` - Загрузка фичи
- `GET /api/v1/features/{id}` - Метаданные фичи
- `GET /api/v1/features/{id}/download` - Скачивание фичи
- `DELETE /api/v1/features/{id}` - Удаление фичи

См. раздел [Feature Store API (Основной API)](#feature-store-api-основной-api) для подробностей.

---

## Примеры использования

### Полный цикл работы с данными

```python
import requests
import polars as pl

BASE_URL = "http://localhost:8000/api/v1"

# 1. Получить список бирж
exchanges = requests.get(f"{BASE_URL}/ingest/exchanges").json()
print(f"Доступно бирж: {len(exchanges['exchanges'])}")

# 2. Получить список символов
symbols = requests.get(
    f"{BASE_URL}/ingest/exchanges/binance/symbols",
    params={"market": "spot"}
).json()
print(f"Символов на Binance: {len(symbols['symbols'])}")

# 3. Запустить загрузку данных
download_req = {
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "market": "spot",
    "timeframe": "1m",
    "start_date": "2024-01-01"
}
response = requests.post(f"{BASE_URL}/ingest/download", json=download_req)
print(response.json())

# 4. Проверить статус загрузки
status = requests.get(f"{BASE_URL}/ingest/status").json()
print(status)

# 5. Получить список загруженных датасетов
datasets = requests.get(
    f"{BASE_URL}/datasets",
    params={"symbol": "BTCUSDT", "limit": 100}
).json()
print(f"Найдено датасетов: {datasets['total']}")

# 6. Предпросмотр данных
if datasets['datasets']:
    preview = requests.get(
        f"{BASE_URL}/datasets/{datasets['datasets'][0]['id']}/preview",
        params={"limit": 10}
    ).json()
    print(f"Колонки: {preview['columns']}")
    print(f"Всего строк: {preview['total_rows']}")

# 7. Загрузить фичи
with open("my_features.parquet", "rb") as f:
    files = {"file": f}
    data = {
        "exchange": "BINANCE",
        "market": "SPOT",
        "symbol": "BTCUSDT",
        "feature_set": "rsi_indicators",
        "version": "1.0.0"
    }
    response = requests.post(f"{BASE_URL}/features/upload", files=files, data=data)
    print(response.json())

# 8. Получить список всех фич
features = requests.get(
    f"{BASE_URL}/features",
    params={"symbol": "BTCUSDT"}
).json()
print(f"Найдено фич: {features['total']}")

# 9. Получить feature sets с версиями
sets = requests.get(
    f"{BASE_URL}/features/sets",
    params={"symbol": "BTCUSDT"}
).json()
for fs in sets["feature_sets"]:
    print(f"{fs['name']}: версии {fs['versions']}")

# 10. Скачать фичу
if features['datasets']:
    feature_id = features['datasets'][0]['id']
    response = requests.get(f"{BASE_URL}/features/{feature_id}/download")
    with open("downloaded.parquet", "wb") as f:
        f.write(response.content)
    print("Фича скачана")
```

---

## Коды ошибок

### Успешные ответы

- `200 OK` - Успешный запрос
- `201 Created` - Ресурс создан

### Ошибки клиента

- `400 Bad Request` - Неверные параметры запроса
- `404 Not Found` - Ресурс не найден
- `422 Unprocessable Entity` - Ошибка валидации данных

### Ошибки сервера

- `500 Internal Server Error` - Внутренняя ошибка сервера

**Пример ошибки:**
```json
{
  "detail": "Feature not found"
}
```

---

## Примечания

1. **Форматы дат**: Все даты в формате ISO 8601 (YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS)
2. **Пагинация**: UI API поддерживает `limit` и `offset` для больших списков
3. **Форматы файлов**: Поддерживаются любые форматы (Parquet, CSV, PDF, JSON, etc.)
4. **Версионирование**: Версия фичи - это строка (например, "1.0.0", "v2", "2024.01.15")
5. **Асинхронные операции**: Загрузка данных с бирж выполняется асинхронно, используйте `/ingest/status` для отслеживания
6. **Checksums**: Автоматически вычисляются при загрузке фич для проверки целостности
7. **CORS**: UI API настроен для работы с фронтендом (CORS разрешен для всех источников)

---

## Автоматическая документация

При запуске сервера доступна интерактивная документация:

- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

Там можно:
- Просмотреть все endpoints
- Протестировать API прямо в браузере
- Увидеть схемы запросов и ответов
