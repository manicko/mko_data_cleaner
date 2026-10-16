
# MKO Data Cleaner
Инструмент для дополнительной обработки выгруженных данных Mediascope (TV / TV_REG). 

Инструмент позволяет удалять лишние строки и создавать пользовательские колонки с данными, 
на основе пользовательского словаря с поисковыми паттернами и правилами.

---

##  Возможности

* Потоковая обработка больших CSV (через **Polars**)
* Гибкая система правил через CSV-словарь
* Локальный SQLite как движок обработки
* LIKE и полнотекстового (FTS5) поиска 
* Пользовательские колонки (категории, бренды и т.д.)
* Поддержка `.gz` 

## Архитектура

```
Импорт (raw data) .csv/.gz  - Polars
   ↓
SQLite (индексы + FTS5)
   ↓
Применение правил (DELETE → REPLACE → ADD)
   ↓
Экспорт (данные с доп. колонками) - Polars
```


---

## 🚀 Быстрый старт


### 1. Инициализация

```bash
mko-data-cleaner init
# или с перезаписью
mko-data-cleaner init --force
```

После инициализации будет создана папка настроек:

```
mko_data_cleaner/
└── settings/
    ├── app_config.yaml
    └── log_config.yaml
```

### 2. Настройка

Основные параметры модуля задаются в `app_config.yaml`.
Настройки словаря для поиска и при в отдельном файле `*.csv`, 
ниже показан пример словаря. 

### 3. Запуск обработки

**Рекомендуемый способ (Python):**

```python
from mko_data_cleaner import initialize_settings, process_data

# 1. Инициализировать настройки один раз. Параметр settings_dir опционален —
#    по умолчанию используется системный каталог platformdirs (внутри него
#    создаётся settings/). Возвращает путь к каталогу с app_config.yaml и
#    log_config.yaml — запомните его и передайте дальше в process_data.
settings_dir = initialize_settings()  # initialize_settings(settings_dir=Path(...))

# 2. Запустить обработку. process_data() возвращает ReportResult.
result = process_data(r"data\mvideo", settings_dir=settings_dir)
```

Параметр `settings_dir` (опциональный) есть и у `initialize_settings()`, и у
`process_data()`; по умолчанию — системный каталог `platformdirs`.

`process_data()` возвращает объект `ReportResult` со следующими полями:

| Поле              | Описание                                                              |
|-------------------|-------------------------------------------------------------------------|
| `report_dir`      | Корневая директория отчёта (`base_path`).                               |
| `import_dir`      | Директория загрузки (`raw_data` из конфига).                            |
| `export_dir`      | Директория экспорта (`clean_data` из конфига).                          |
| `db_path`         | Путь к временной SQLite-базе.                                           |
| `null_data_files` | Список `null_data_*.csv.gz` — строки без совпадений по словарю.         |
| `started_at`      | Время начала (`datetime`).                                              |
| `finished_at`     | Время окончания (`datetime`).                                           |
| `duration`        | Полное время выполнения в секундах (`float`).                           |
| `timings`         | Тайминги по стадиям: `{import, mapping, export}`, сек.                 |
| `rows_loaded`     | Количество загруженных строк.                                           |
| `rows_exported`   | Количество строк в экспорте.                                            |

> **Важно:** `process_data()` загружает настройки лениво. Если `initialize_settings()`
> не вызван (или `settings_dir` указывает в пустой каталог), выбрасывается
> `SettingsNotInitializedError` — подкласс `FileNotFoundError`. Сообщение об ошибке
> именует `mko_data_cleaner.app.initialize_settings()` как способ исправить ошибку.

**Через CLI:**

```bash
# настройки в системном каталоге (по умолчанию)
mko-data-cleaner init                     # создать настройки
mko-data-cleaner init --force             # перезаписать существующие
mko-data-cleaner run path/to/your_report_folder
mko-data-cleaner list-settings

# явный каталог настроек (переопределяет системный)
mko-data-cleaner init --settings-dir C:\my\settings
mko-data-cleaner run --settings-dir C:\my\settings path/to/your_report_folder
mko-data-cleaner list-settings --settings-dir C:\my\settings
```

Опция `--settings-dir DIRECTORY` есть у команд `init`, `run` и `list-settings`.
Она переопределяет каталог пользовательских настроек: по умолчанию используется
системный каталог `platformdirs` (внутри него создаётся `settings/` с
`app_config.yaml` и `log_config.yaml`).

### 4. Пример ноутбука (Jupyter)

Готовый runnable‑пример, демонстрирующий полный цикл
`initialize_settings → process_data → ReportResult`:
[`examples/notebooks/mko_data_cleaner_pipeline_example.ipynb`](examples/notebooks/mko_data_cleaner_pipeline_example.ipynb)
(создаёт синтетический отчёт в‑cell и разбирает результат).

Установить pinned‑релиз (тег `v0.2.0`):

```
%pip install "git+https://github.com/manicko/mko_data_cleaner.git@v0.2.0"
```

---

## Формат входных данных

Пример сырых CSV-файлов Mediascope (строго UPPERCASE):

```csv
YEAR;MONTH;ADVERTISERS;BRANDS;SUBBRANDS;ARTICLE LIST3;ARTICLE LIST4;ADID;MEDIA;COST RUB;QUANTITY
2024;01;EXAMPLE CORP;APPLE INC;IPHONE 16;SMARTPHONES;ELECTRONICS;123456789;TV;1500000;25
2024;01;EXAMPLE CORP;APPLE INC;MACBOOK PRO;NOTEBOOKS;ELECTRONICS;123456790;TV;3200000;12
2024;02;FAKE RETAIL LTD;SAMSUNG;GALAXY S25;SMARTPHONES;ELECTRONICS;987654321;OUTDOOR;450000;8
```

---

## Формат пользовательского словаря `merged_dictionary.csv`

Названия колонок не важны, важен порядок - индексы колонок задаются в файле конфигурации:

| action | match | search_column_idx | value | term          | MACRO CATEGORY | MARKETPLACE | BRAND CLEANED |
|--------|-------|-------------------|-------|---------------|----------------|-------------|---------------|
| r      | f     | 3                 |       | APPLE INC     | ELECTRONICS    |             | APPLE         |
| r      | p     | 5                 |       | SMART         |                |             | SMARTPHONES   |
| a      | f     | 2                 |       | YANDEX        |                |             | YANDEX MARKET |
| d      | f     | 3                 |       | DELETE ME     |                |             |               |
| r      | fts   | 3\|5              |       | OZON\|MARKET  | MARKETPLACE    | MARKETPLACE | OZON          |

> Позиции колонок задаются в `app_config.yaml`
> (`dict_file_settings.col_indexes`, нумерация с 0). В базовой конфигурации
> `term` читается из 4-й колонки, а колонка 3 (`value`) при применении правил
> не используется. Если в вашем словаре нет промежуточной колонки, установите
> в конфиге `term: 3` (и уберите колонку из примеров выше).

**Значения action:**  
- `r` — REPLACE (добавление/замена значения в колонке)  
- `a` — ADD (объединяет (merge) теги всех совпавших правил в колонку, через разделитель)  
- `d` — DELETE (удаление строки)  

**Значения match:**  
- `f` — полное совпадение  
- `p` — частичное (`%term%`)  
- `s` — начинается с (`term%`)  
- `e` — заканчивается на (`%term`)   
- `fts` — полнотекстовый поиск

`search_column_idx` — 0-based индекс колонки (для FTS — через разделитель `|`).

---
## Логика применения правил

Правила применяются **строго в порядке**:
1. **DELETE** (`d`) — полностью удаляет совпавшие строки.
2. **REPLACE** (`r`) — заменяет значение в указанной колонке.
3. **ADD** (`a`) — **объединяет** теги всех совпавших правил в одну колонку через разделитель.

### Особенности правила ADD

- Теги всех совпавших правил `a` **объединяются** (merge) в одну колонку.
- Все теги автоматически становятся **уникальными**.
- Теги **сортируются по алфавиту**.
- Разделитель задаётся в `app_config.yaml` (`dict_file_settings.add_separator`, по умолчанию `", "`).

Принцип работы ADD на основе **пользовательской** добавляемой колонки:

| Предыдущее значение (от других правил ADD) | Добавляемое значение | Результат после ADD                     |
|-------------------------------------------|----------------------|-----------------------------------------|
| ELECTRONICS                               | SMARTPHONES          | ELECTRONICS, SMARTPHONES                |
| BRAND1, BRAND3                            | BRAND2               | BRAND1, BRAND2, BRAND3                  |
| YANDEX MARKET                             | YANDEX MARKET        | YANDEX MARKET (дубликат удалён)         |

> **Известное ограничение:** если REPLACE (`r`) и ADD (`a`) пишут в одну и ту
> же колонку для одной и той же строки, то текущая реализация ADD
> перезаписывает значение, записанное REPLACE — в колонке остаются только теги
> ADD. Подробности и статус — в
> [`docs/00-overview/open-questions.md`](docs/00-overview/open-questions.md) (D-2).

---

## Что делает инструмент

1. Загружает данные из поддиректории (`raw_data`), указанной в конфиге.
2. Создаёт временную SQLite-базу с индексами и FTS5 (при необходимости).
3. Применяет правила в порядке: **DELETE → REPLACE → ADD**.
4. Сохраняет нераспознанные строки в `null_data_*.csv.gz`.
5. Экспортирует данные с добавленными колонками в поддиректорию, указанную в конфиге (`clean_data`).


### Нераспознанные данные

```
null_data_*.csv.gz
```

Используются для:

* дообучения словаря
* поиска пропущенных кейсов


## Рекомендации

* Делать словарь итеративно (через `null_data`)
* Использовать FTS только при необходимости
* Использовать индексы (`index_column`) - колонка с `id` рекламы.

---

## Документация / Documentation

Full project documentation lives in [`docs/00-overview/`](docs/00-overview/):

| Document | Contents |
|---|---|
| [`product-overview.md`](docs/00-overview/product-overview.md) | What the product is for, who uses it, owner-confirmed intent |
| [`current-behavior.md`](docs/00-overview/current-behavior.md) | Entry points, main scenario step by step, rule engine, errors, configuration |
| [`data-flow.md`](docs/00-overview/data-flow.md) | Input/output formats, transformations, handling of missing and invalid values |
| [`architecture.md`](docs/00-overview/architecture.md) | Components, interactions, diagrams, technical risks |
| [`open-questions.md`](docs/00-overview/open-questions.md) | Owner decisions, discrepancies, open questions |

Start with `product-overview.md`; developers should then read
`current-behavior.md` and `architecture.md`.

---

✨ Чистых данных!
---
