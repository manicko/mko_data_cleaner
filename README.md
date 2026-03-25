
# MKO Data Cleaner

Инструмент для дополнительной обработки выгруженных данных Mediascope (TV / TV_REG). 

Поиск строк по заданному паттерну и удаление строк или добавление колонок с пользовательскими значениями.

Поиск и применение правил осуществляется на основе словаря пользователя.

---

## Быстрый старт

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

Все пути к данным (`raw_data`, `clean_data`, словарь и т.д.) указываются **в `app_config.yaml`**.

### 2. Запуск обработки

**Рекомендуемый способ (Python):**

```python
from mko_data_cleaner.app import process_data

process_data(r"data\mvideo")
```

**Альтернатива через core:**

```python
from mko_data_cleaner.core.app_service import app_service

app_service.run_report(r"data\mvideo")
```

**Через CLI:**

```bash
mko-data-cleaner run path/to/your_report_folder
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

| action | match | search_column_idx | term          | MACRO CATEGORY | MARKETPLACE | BRAND CLEANED |
|--------|-------|-------------------|---------------|----------------|-------------|---------------|
| r      | f     | 3                 | APPLE INC     | ELECTRONICS    |             | APPLE         |
| r      | p     | 5                 | SMART         |                |             | SMARTPHONES   |
| a      | f     | 2                 | YANDEX        |                |             | YANDEX MARKET |
| d      | f     | 3                 | DELETE ME     |                |             |               |
| r      | fts   | 3\|5              | OZON\|MARKET  | MARKETPLACE    | MARKETPLACE | OZON          |

**Значения action:**  
- `r` — REPLACE (добавление/замена значения в колонке)  
- `a` — ADD (добавление значения к колонке, через разделитель)  
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
3. **ADD** (`a`) — **добавляет** новое значение к уже существующему через разделитель.

### Особенности правила ADD

- Новое значение **добавляется**, а не заменяет текущее.
- Все теги автоматически становятся **уникальными**.
- Теги **сортируются по алфавиту**.
- Разделитель задаётся в `app_config.yaml` (`dict_file_settings.add_separator`, по умолчанию `", "`).

Принцип работы ADD на основе **пользовательской** добавляемой колонки:

| Предыдущее значение | Добавляемое значение | Результат после ADD                     |
|---------------------|----------------------|-----------------------------------------|
| ELECTRONICS         | SMARTPHONES          | ELECTRONICS, SMARTPHONES                |
| BRAND1, BRAND3      | BRAND2               | BRAND1, BRAND2, BRAND3                  |
| YANDEX MARKET       | YANDEX MARKET        | YANDEX MARKET (дубликат удалён)         |

---

## Что делает инструмент

1. Загружает данные из поддиректории (`raw_data`), указанной в конфиге.
2. Создаёт временную SQLite-базу с индексами и FTS5 (при необходимости).
3. Применяет правила в порядке: **DELETE → REPLACE → ADD**.
4. Сохраняет нераспознанные строки в `null_data_*.csv.gz`.
5. Экспортирует данные с добавленными колонками в поддиректорию, указанную в конфиге (`clean_data`).

---
✨ Чистых данных!
---