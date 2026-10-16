# План перехода на polars 2.0

- **Дата:** 2026-10-15 · **HEAD на момент анализа:** `61a5d95` (+ рабочие коммиты `efd18b5`, ветка `feat-pynote-support`) · Python: 3.12–3.14 (uv)
- **Источник фактов:** аудит использования pyarrow/adbc + два исследования с эмпирическими замерами (scratch-венвы: polars 1.38.1 / 1.44.2 / 2.0.0 × pyarrow present/absent/broken, adbc-driver-sqlite 1.10.0/1.12.0) + прямые проверки Tech Lead'а
- **Стек записи — только polars** (`DataFrame.write_database(engine="adbc")` → SQLite). pandas/sqlalchemy/голый sqlite-запись в проекте отсутствуют — это мёртвый residue миграции 2026 года, не рассматриваются.

---

## 1. Резюме (TL;DR)

1. **Переход на polars 2.0 дешёвый: ровно 2 обязательные правки кода** (`rechunk` и `read_csv_batched`), они **version-agnostic** — работают и на 1.38.1, и на 2.0.0 (проверено экспериментально). Миграцию можно сделать, не меняя версию.
2. **КРИТИЧНО прямо сейчас:** текущий HEAD (`pyproject.toml` cap `polars>=1.38.1,<3.0.0` — ослаблен коммитом `61a5d95`) при свежей установке тянет **polars 2.0.0 и падает**:
   `polars.exceptions.ArgumentRemovedError: the argument 'rechunk' for 'read_csv' has been removed in version 2.0` (воспроизведено Tech Lead'ом: `uv pip install .` в чистом венве → `polars==2.0.0` → smoke-скрипт падает в `CSVWorker.get_csv_headers`). `uv.lock` ещё держит `<2.0.0`, поэтому dev/CI (`uv sync --frozen`) живы, но `pip install`/fresh-resolve — сломаны. **Это чинится либо задачей T0 (вернуть cap), либо задачами T1–T2 (мигрировать код).**
3. **Улучшения 2.0 для нашего паттерна минимальны:** главный bottleneck (`write_database append` — ~70% времени импорта) не ускоряется (в замере даже +6% медленнее); `write_csv` −9%; экспícort из SQLite быстрее; streaming-память заметно лучше (+13 MB vs +68 MB на `collect_batches`). Малые `replace`-записи в одном замере ~2× медленнее — **требует проверки на реальном словаре**.
4. **pyarrow НЕ нужен пути записи** ни в 1.38.1, ни в 1.44.2, ни в 2.0.0 (экспериментально; актуальные доки polars устарели — они описывают `adbc-driver-manager < 1.6.0`; наш минимум — 1.10.0). pyarrow остаётся объявленной зависимостью до отдельного решения (T7), guard `PyArrowInstallationError` (efd18b5) остаётся.
5. **Рекомендация:** выполнить T0→T2 (сделать код version-agnostic, не повышая версию) → валидация на 2.0.0 (T5) → затем решение владельца: поднимать cap сейчас или дождаться 2.1/2.2. Если ждём — cap оставляем `<2.0.0`; всё равно код останется совместимым с обеими линейками.

---

## 2. Текущее состояние (факты аудита)

| Что | Факт |
|---|---|
| Путь записи | 4 сайта `DataFrame.write_database(table_name=..., connection=db_worker.db_adb_con, engine="adbc", if_table_exists="append"/"replace")` в `core/app_service.py` (`_import_data` — чанками 10k; `fts_temp_tbl`, `temp_tbl`, `mapping_table` в цикле `generate_rules_blocks`), после каждой записи `db_adb_con.commit()`; `db_adb_con = adbc_driver_sqlite.dbapi.connect(...)` из `core/db_service.py` |
| Второе соединение | stdlib `sqlite3` (`db_con`) — все DDL/queries/triggers/export; WAL (`synchronous=OFF`, `cache_size=-100000`); `close()` делает `wal_checkpoint(FULL)` |
| Чтение | `pl.read_csv_batched(new_columns=..., **reader_settings)` в `CSVWorker._read_csv_in_chunks` (batch 10k); `pl.read_csv(**reader_settings)` в `get_csv_headers`/`check_date_column`/`get_dictionary`/`get_merged_dictionary` |
| `reader_settings` (`settings/app_config.yaml` → `PolarsReadCSV`) | separator `;`, encoding utf8, `decimal_comma: True`, `infer_schema_length: False`, **`rechunk: False`** (удалён в 2.0) |
| Эксп ухэд | stdlib `cursor.fetchmany` → `pl.DataFrame(rows, orient="row", schema_overrides={...pl.Utf8})` → `df.write_csv(compression="gzip", include_bom=True, separator=";")` |
| Версии | pyproject: `polars>=1.38.1,<3.0.0` (HEAD); committed uv.lock: `<2.0.0`; **в рабочем дереве незакоммиченная правка uv.lock на `<3.0.0`** |
| Мёртвый residue (мешает читаемости, не функционал) | `DATA_TO_SQL_PARAMS` + `DBWorker.data_chunk_to_sql` (pandas `to_sql`, 0 вызовов); комментарий «pandas CSV reader» в `app_config.yaml`; зависимости `pandas`, `pydantic-settings` (0 импортов); закомментированный `sqlalchemy` в `pyproject.toml` |
| Guard pyarrow | `_ensure_pyarrow_importable` (`efd18b5`): `find_spec` → absent = no-op; spec есть + import падает → `PyArrowInstallationError` ( actionable `--force-reinstall` ) |

---

## 3. Анализ влияния polars 2.0 (проверено эмпирически на 2.0.0)

### 3.1 Ломается (обязательные правки)

| # | API | Где в проекте | Симптом на 2.0 | Правка |
|---|---|---|---|---|
| 1 | `read_csv(..., rechunk=False)` | 4 сайта в `core/csv_service.py` через `**reader_settings` (`get_csv_headers`, `check_date_column`, `get_dictionary`, `get_merged_dictionary`) + `PolarsReadCSV.rechunk` (`core/models.py`) + `rechunk: False` (`settings/app_config.yaml`) | `ArgumentRemovedError: the argument 'rechunk' for 'read_csv' has been removed in version 2.0` | Удалить поле `rechunk` из модели, конфига; на 1.x поведение идентично (дефолт — без rechunk) |
| 2 | `pl.read_csv_batched(...)` + `reader.next_batches(1)` | `CSVWorker._read_csv_in_chunks` | `AttributeRemovedError: 'read_csv_batched' was removed in version 2.0; use 'scan_csv'...` | Заменить на `pl.scan_csv(...).collect_batches(chunk_size=CHUNK_SIZE)` (см. §4.2) |

Deprecated уже на 1.x (предупреждения в тестах): `read_csv_batched` (since 1.37.0), `rechunk` (since 1.43.2) — миграция неизбежна в любом случае.

### 3.2 Работает без изменений (проверено)

- Все 4 сайта `write_database` (сигнатура, `if_table_exists`, `engine="adbc"`, connection-object, возврат `int` для `rows_loaded`);
- `df.write_csv(compression="gzip", include_bom=True, separator=";", decimal_comma=True)`;
- `pl.DataFrame(rows, orient="row", schema_overrides={...: pl.Utf8})`; `pl.Utf8` — алиас `pl.String`;
- `pl.concat` (вертикальный и `how="horizontal"` при равной высоте), `with_row_index`, `from_dicts`, `when/then/otherwise`, `concat_str`, `filter/join/sort/unique`, `is_empty/height/width/columns/iter_rows`, `infer_schema_length=False` (bool→0 — работает во всех версиях).

---

## 4. pyarrow / adbc — выводы

1. **Путь записи не требует pyarrow ни в одной версии.** В polars `write_database`: `data = self if driver_manager_version >= (1, 6) else self.to_arrow()` — при `adbc-driver-manager >= 1.6.0` (наш минимум 1.10.0, проверено на 1.12.0) polars передаёт DataFrame через Arrow C stream (PyCapsule), pyarrow не нужен. Docs полars («PyArrow is required for SQLite … when passing an instantiated connection object») описывают старые `adbc-driver-manager < 1.6.0`. Подтверждено экспериментами: pyarrow absent → `write_database append/replace` OK на 1.38.1 / 1.44.2 / 2.0.0.
2. **Единственный сломанный сценарий** — pyarrow «есть на диске, но не импортируется»: polars решает доступность через `find_spec` (spec есть → `True`), `adbc_driver_manager` делает реальный import → `adbc_get_info()` → `ProgrammingError: This API requires PyArrow to be installed`. Это ловит guard `efd18b5` → понятный `PyArrowInstallationError` с рецептом `--force-reinstall`. **Guard остаётся.**
3. **Нюанс для guard-docstring:** в изоляции pyarrow-absent conn-obj на 1.x работал стабильно (в т.ч. полный e2e smoke-скрипт — 3+ прогона), но в одном комплексном скрипте исследователь зафиксировал flaky-падение на 1.x. Вывод: guard корректно ловит broken-case; его docstring («works without pyarrow, verified on polars >= 1.38.1») следует уточнить (T4), а pyarrow остаётся объявленной зависимостью до отдельного решения (T7).
4. **adbc остаётся** механизмом записи (замена не найдена и не требуется): URI-форма (`sqlite:///...`) тоже работает и без pyarrow, но на 1.x даёт per-call overhead на малых replace-блоках (~2× на батчах 2k в замере) без иной выгоды; на 2.0 conn-obj и так стабилен без pyarrow. **Не переключаемся.**

---

## 5. Производительность (замеры, ~500k строк CSV / 10k-чанки)

| Операция | 1.38.1 | 1.44.2 | 2.0.0 | Вывод |
|---|---|---|---|---|
| read_csv (full) | 87 ms | 125 ms | 114 ms | без выигрыша |
| scan/collect_batches (streaming) | 99 ms | 96 ms | 97 ms | нет изменения |
| **write_database append (bottleneck)** | 1595 ms | 1610 ms | 1692 ms | **без улучшения (+6% в одном замере)** |
| write_csv gzip | 1316 ms | 1282 ms | 1196 ms | −9% |
| экспícort (sqlite3 cursor → CSV) | 61 ms | 41 ms | 35 ms | −43% |
| малый replace (2–1000 строк, conn-obj) | 3–5 ms | 3–5 ms | ~11 ms | **~2× медленнее — проверить на реальном словаре** |
| память: streaming collect_batches | +68 MB | — | +13 MB | заметно лучше |
| память: освобождение после `del df` | нет | — | да | лучше |

**Итог по перфу:** переход на 2.0 не решает главное узкое место (import). Выигрыши — экспícort, gzip-запись, память. Цена — возможный регресс малых replace (проверить T5).

---

## 6. План перехода (задачи)

Обозначения: semantic units, без номеров строк. Каждая задача — самостоятельный reviewable блок; везде формат: файлы → изменения → приёмка → верификация.

### T0. Остановить fresh-install поломку (срочно, до любой миграции)
- **Файлы:** `pyproject.toml`, `uv.lock`.
- **Изменения:** вернуть зависимость `polars` к `polars>=1.38.1,<2.0.0` в `pyproject.toml` и синхронизировать `uv.lock` (откатить незакоммиченную правку lock на `<3.0.0`; либо пере-залочить `uv lock` после правки pyproject).
- **Причина:** HEAD с cap `<3.0.0` → fresh install тянет polars 2.0.0 → краш `ArgumentRemovedError` (воспроизведено). CI install-smoke/pip-install пути сломаны.
- **Приёмка:** в чистом венве `uv pip install dist/*.whl` (или `.`) resolved `polars 1.x`; `scripts/smoke_install.py` — все `[PASS]`.
- **Rollback:** вернуть cap `<3.0.0` (но тогда обязательно T1+T2).

### T1. Убрать `rechunk` (version-agnostic)
- **Файлы:** `core/models.py` (`PolarsReadCSV`: удалить поле `rechunk`), `settings/app_config.yaml` (удалить ключ `rechunk` из `read_settings.from_csv`).
- **Приёмка:** поведение чтения идентично на 1.38.1 (тесты e2e и cut); forward-compatible с 2.0.
- **Верификация:** `uv run pytest`; при наличие вевенва polars 1.44.2 — отсутствие DeprecationWarning по rechunk.

### T2. Мигрировать чтение чанками на `scan_csv().collect_batches()` (version-agnostic)
- **Файлы:** `core/csv_service.py` — метод `_read_csv_in_chunks`.
- **Изменение (code sketch; проверить в реализации):**
  ```python
  reader = pl.scan_csv(csv_file, new_columns=headers, **self.reader_settings)
  for batch in reader.collect_batches(chunk_size=self.CHUNK_SIZE):
      yield batch
  ```
- **Приёмка:** фрагмент работает на polars 1.38.1 И 2.0.0 с нашим `reader_settings` без `rechunk` (Tech Lead проверил оба: batches отдаются по `chunk_size`, счёт строк совпадает); семантика совпадает с `read_csv_batched` (батчи по 10k); e2e-тест и smoke зелёные.
- **Риски:** ленивый `scan_csv` (ошибки файла могу surfaced позже) — проверить поведение при недостающем/битом файле (тест на ошибку чтения); порядок батчей = порядок файла (наш случай).

### T3. Удалить мёртвый pandas-residue (чистота)
- **Файлы:** `core/db_service.py` (удалить `DATA_TO_SQL_PARAMS` и метод `data_chunk_to_sql`), `settings/app_config.yaml` (убрать устаревший комментарий про pandas), `pyproject.toml` (опционально решение по `pandas`/`pydantic-settings` — вне этого плана, фиксируем как known-residue).
- **Приёмка:** grep: 0 ссылок на удалённые символы; `uv run pytest` зелёный.

### T4. Уточнить guard pyarrow (docstring/точность)
- **Файлы:** `core/db_service.py` (`_ensure_pyarrow_importable`).
- **Изменение:** привести docstring в соответствие с фактами: guard ловит состояние «pyarrow есть на диске, но не импортируется» → `PyArrowInstallationError` с рецептом; полное отсутствие pyarrow guard пропускает (на 2.0 conn-obj стабилен без pyarrow; на 1.x — минимальный риск flaky, pyarrow остаётся объявленной зависимостью).
- **Приёмка:** существующие тесты guard'а (`tests/test_db_service.py`) зелёные; текст не противоречит экспериментам.

### T5. Валидация на polars 2.0.0 (scratch, без изменения репы)
- **Где:** отдельный венв `uv venv --python 3.12` + `uv pip install "polars==2.0.0" .` (локальное дерево) или `uv pip install -e .` с временным override.
- **Проверить:** (а) `scripts/smoke_install.py` — все 6 `[PASS]`; (б) `uv run pytest` с polars 2.0.0 (временно через `--with polars==2.0.0` или отдельный венв с pytest); (в) **реальный словарь**: малые replace-записи `mapping_table`/`fts_temp_tbl` (ретест регресса ~2×) на объёме реального мерча; (г) выполнение примера `examples/notebooks/*.ipynb` (nbclient) на 2.0.0; (д) свежерезолв-инсталляция: венв без lock → polars 2.0.0 → e2e зелёный.
- **Приёмка:** все проверки зелёные + запись выводов (времена/счётчики) в отчёте; либо зафиксированные блокеры с планом.

### T6. Переключение cap (после зелёной T5)
- **Файлы:** `pyproject.toml` → `polars>=2.0.0,<3.0.0`; `uv lock`.
- **Приёмка:** `uv run pytest` / ruff / build / `twine check --strict` / `check-wheel-contents` / fresh-venv install smoke (pip-install путь) — всё зелёное; тесты e2e и notebook на 2.0.0.

### T7. Решение по pyarrow (опционально, после T6)
- **Варианты:** оставить как есть (рекомендуется минимально); либо перевести в optional extra. Триггер для optional:Guard-проверка + отсутствие других consumer'ов + зелёные no-pyarrow прогоны на целевой версии.
- **Не входит:** удаление guard (он ловит broken-install и остаётся полезен).

### T8. Релиз и документация
- Версия `0.2.0` (minor: миграция рантайма + обновление floor polars), тег, README/settings-дока: обновить минимальную версию polars и (если применимо) упоминание pyarrow. Решение — за владельцем.

---

## 7. Альтернативы, отклонённые (с причинами)

| Альтернатива | Почему отклонена |
|---|---|
| Остаться на 1.x «навсегда», ничего не менять | `read_csv_batched`/`rechunk` deprecated и удалены в 2.0; миграция дешёвая и version-agnostic — откладывание только накапливает риск (как сейчас: cap `<3.0.0` ломает fresh install) |
| URI-форма write_database (`sqlite:///...`) вместо connection-object | На 1.x: нет выгоды (всё равно требуется pyarrow-introspection в пуле) + per-call overhead на малых replace (~2×). На 2.0 conn-obj стабилен без pyarrow |
| pandas `to_sql` / sqlalchemy / голый sqlite-запись | Не наш стек (мёртвый residue); требовали бы новых зависимостей и изменения 4 сайтов записи |
| Поднять cap сразу, без миграции | Воспроизведённый краш `ArgumentRemovedError` на свежем резолве — регресс для всех новых установок |

---

## 8. Риски и митигации

| Риск | Уровень | Митигация |
|---|---|---|
| 2.0.0 молодая (релиз 2026-10-06) | Средний | T5 валидация на реальных данных; cap можно оставить `<2.0.0` до 2.1/2.2 — код уже будет совместим |
| Малые replace ~2× медленнее на 2.0 | Средний | T5(в): замер на реальном словаре; если критично — оставить cap `<2.0.0` |
| `scan_csv` — ленивое чтение: ошибки всплывают позже | Низкий | T2: тест на поведение при битом/пустом CSV |
| pyarrow-absent flaky на 1.x (conn-obj) | Низкий | pyarrow остаётся зависимостью; guard ловит broken-case; T4 фиксит докстринг |
| Незакоммиченная правка `uv.lock` (`<3.0.0`) | Низкий | T0 синхронизирует lock с cap |
| Сломанный `pip install` путь прямо сейчас | **Высокий** | T0 немедленно |

---

## 9. Открытые вопросы / решения владельца

1. Дожидаться 2.1/2.2 или переходить на 2.0.0 после T5? (Рекомендация: выполнить T0–T4 сейчас — код станет version-agnostic; cap решать после T5.)
2. Версия релиза после миграции: `0.2.0`?
3. Существование release-механики: тег v0.2.0 + GitHub (как для v0.1.0)?

---

## 10. Definition of Done (DoD)

- [ ] T0: fresh install резолвит polars 1.x, smoke — все `[PASS]`.
- [ ] T1–T2: код читает CSV через `scan_csv().collect_batches()` без `rechunk`; `uv run pytest` зелёный на 1.38.1; фрагмент проверен на 2.0.0.
- [ ] T3: мёртвый pandas-residue удалён; grep чист.
- [ ] T4: guard-докстринг соответствует экспериментам.
- [ ] T5: полная валидация на 2.0.0 (smoke, pytest, notebook, реальный словарь, fresh-install) — выводы приложлены.
- [ ] T6 (если одобрено): cap `>=2.0.0,<3.0.0`, lock синхронён, CI-эквивалент локально зелёный.
- [ ] T8 (если одобрено): релиз/дока обновлены.
