# s3-dedup

`s3-dedup` — служба на Go 1.20 для поиска и устранения дубликатов в aws-совместимом объектном хранилище. Программа сканирует настроенные bucket/prefix, потоково вычисляет хеш содержимого, хранит состояние в SQLite и формирует JSON-отчёт.

В режиме `report_only` S3 не изменяется. В режиме `pointer` содержимое переносится в `blobs/<hash>`, а исходный логический ключ может быть заменён небольшим JSON pointer object. Blob удаляется только после того, как число ссылок на него стало равно нулю.

## Возможности

- AWS-совместимое API через `minio-go`;
- сканирование нескольких bucket/prefix;
- потоковое хеширование `sha256` или `sha512`;
- параллельная обработка через настраиваемое число workers;
- режимы `report_only` и `pointer`;
- SQLite cache с refcount и восстановлением после перезапуска;
- безопасная проверка исходного объекта перед заменой;
- удаление только неиспользуемых blobs;
- JSON report после каждого прохода;
- Windows Service и Linux systemd;
- кросс-сборка Windows/Linux;
- тесты идемпотентности и восстановления после частичных ошибок.

## Структура проекта

```text
cmd/s3-dedup/       main, запуску CLI
internal/app/       создание S3 client, cache, logger и scanner
internal/cache/     SQLite кеш
internal/command/   CLI и service
internal/config/    Парсинг конфига yaml и переменных окружения
internal/hashing/   потоковое хеширование
internal/logger/    файловый/stdout logger
internal/pointer/   формат JSON pointer
internal/report/    Чтение и запись JSON отчета
internal/s3/        обёртка над клиентом minio-go
internal/scanner/   сканирование и дедупликация
configs/            файлы конфигурации
docs/               описание схемы дедупликации
testdata/control/   фиксированный набор данных для demo
scripts/            сценарий Docker demo
```

## Требования

Для сборки и тестов:

- Go 1.20;

Для `make demo` дополнительно нужны:

- Docker Desktop или Docker Engine;
- Docker Compose v2;
- каталог `testdata/control` с контрольными файлами.

## Сборка и тесты

```bash
make test
make build
```

`make test` выполняет:

```bash
go test ./...
go vet ./...
```

`make build` создаёт:

```text
bin/s3-dedup-linux-amd64
bin/s3-dedup-windows-amd64.exe
```

## Конфигурация

Пример YAML:

```yaml
s3:
  endpoint: "localhost:9000"
  region: "us-east-1"
  access_key: ""
  secret_key: ""
  use_path_style: true
  buckets:
    - name: "my-bucket"
      prefix: "input/"

dedup:
  hash_algo: "sha256"
  min_size_bytes: 4096
  blob_prefix: "blobs/"
  mode: "report_only"
  delete_originals: false

cache:
  backend: "sqlite"
  path: "/var/lib/s3-dedup/state.db"

schedule:
  scan_interval: "1h"
  workers: 8

logging:
  level: "info"
  file: "/var/log/s3-dedup/service.log"
```

### Поля конфигурации

| Поле | Назначение |
|---|---|
| `s3.endpoint` | S3 endpoint в формате `host:port` без схемы |
| `s3.region` | Регион S3 |
| `s3.access_key` | Access key; может быть пустым при использовании environment variable |
| `s3.secret_key` | Secret key; может быть пустым при использовании environment variable |
| `s3.use_path_style` | Использовать path-style bucket lookup |
| `s3.buckets[].name` | Bucket для сканирования |
| `s3.buckets[].prefix` | Prefix внутри bucket |
| `dedup.hash_algo` | `sha256` или `sha512` |
| `dedup.min_size_bytes` | Объекты меньше значения не хешируются и не подвергаются дедупликации |
| `dedup.blob_prefix` | Prefix для физических blobs, например `blobs/` |
| `dedup.mode` | `report_only` или `pointer` |
| `dedup.delete_originals` | В `pointer` mode разрешает замену исходного объекта pointer object |
| `cache.backend` | В текущей реализации только `sqlite` |
| `cache.path` | Путь к .db файлу SQLite |
| `schedule.scan_interval` | Интервал сканирования в формате |
| `schedule.workers` | Число workers; значения `<= 0` заменяются на `1`, максимум — `2 * NumCPU` |
| `logging.level` | `debug`, `info`, `warn` или `error` |
| `logging.file` | Log file; пустое значение или `-` означает stdout |

Родительский каталог для log file должен существовать. Каталог SQLite cache создаётся автоматически.

### Переменные окружения

Environment variables имеют приоритет над значениями YAML:

```text
S3_ACCESS_KEY
S3_SECRET_KEY
```

## CLI

### Один проход

```bash
s3-dedup scan-once --config configs/config.yaml
```

По умолчанию report записывается в `report.json` текущего каталога. Другой путь:

```bash
s3-dedup scan-once --config configs/config.yaml --out build/report.json
```

### Периодический запуск

```bash
s3-dedup run --config configs/config.yaml --out report.json
```

Первый scan запускается сразу. Следующие scans запускаются через `schedule.scan_interval` после завершения предыдущего прохода.

### Чтение последнего report

```bash
s3-dedup report --out report-copy.json
```

Текущая команда `report` читает `./report.json` последнего прохода, печатает его и записывает копию в `--out`. Поэтому её нужно запускать из каталога, содержащего исходный `report.json`.

### Справка

```bash
s3-dedup --help
s3-dedup scan-once --help
s3-dedup run --help
s3-dedup install --help
```

## Схема дедупликации

Физическая идентичность blob определяется парой `(bucket, hash)`. Blobs разных buckets не объединяются.

Pointer object хранится под исходным logical key и имеет content type `.json+pointer`:

```json
{
  "blob_bucket": "my-bucket",
  "blob_key": "blobs/84d89877f0d4041e...",
  "hash": "84d89877f0d4041e...",
  "size": 1048576,
  "content_type": ".objectContentType"
}
```

### `report_only`

1. Объект читается через `GetObject`.
2. Хеш вычисляется потоково через `io.Reader`.
3. Hash и связь логического объекта с blob записываются в SQLite.
4. S3 objects не изменяются и blobs не создаются.

### `pointer`

1. Содержимое потоково хешируется и одновременно записывается во временный файл.
2. Проверяется наличие `blob_prefix + hash`.
3. Если blob отсутствует, он загружается первым и проверяется его размер.
4. Исходный object повторно проверяется по ETag, size и last_modified.
5. Если object не изменился и `delete_originals: true`, logical key заменяется pointer JSON.
6. Записанный pointer проверяется через `StatObject`.
7. Связь и refcount атомарно обновляются в SQLite.

В текущей реализации `pointer` mode переводит в pointer каждый подходящий original object, включая первое уникальное содержимое. Поэтому `objects_relinked` может быть больше `duplicates_found`.

## Консистентность и восстановление

- Blob записывается до замены logical object.
- Существующий blob проверяется по размеру.
- Source повторно проверяется перед заменой; изменившийся объект не заменяется.
- Pointer принимается только при корректном JSON, ожидаемом blob key и существующем blob нужного размера.
- Обновление объектов и blob в кеше выполняется за одну транзакцию SQLite.
- Изменение hash объекта увеличивает refcount нового blob и уменьшает refcount старого.
- После полного listing `FinalizeScope` удаляет из cache отсутствующие logical objects и уменьшает их refcount.
- Garbage collection выполняется только после прохода без ошибок обработки объектов и удаляет только blobs с `ref_count = 0`.
- Сначала удаляется S3 blob, затем соответствующая запись cache.
- Повторный scan использует ETag, size и last_modified, поэтому неизменившиеся objects не хешируются повторно.
- После сбоя pointer или original будет повторно обнаружен и зарегистрирован при следующем scan.
- Ошибка обработки одного объекта увеличивает количество ошибок для отчено, но не останавливает сканирования.
- Сканирование останавливает только ошибка листинга, финализация кеша или ошибка сборщика мусора

## JSON report

Пример:

```json
{
  "scan_started": "2026-06-16T10:00:00Z",
  "scan_finished": "2026-06-16T10:07:31Z",
  "objects_scanned": 120,
  "unique_blobs": 6,
  "duplicates_found": 114,
  "bytes_reclaimable": 560000,
  "bytes_reclaimed": 430000,
  "objects_relinked": 120,
  "errors": 0,
  "mode": "pointer"
}
```

| Поле | Значение |
|---|---|
| `scan_started` | UTC start time |
| `scan_finished` | UTC finish time |
| `objects_scanned` | Число подходящих логических объектов, увиденных в проходе |
| `unique_blobs` | Число уникальных `(blob_bucket, hash)` в кеше |
| `duplicates_found` | Сумма `object_count - 1` для содержимого с несколькими ссылками |
| `bytes_reclaimable` | Оценочное значение возможного освобождения хранилища в байтах |
| `bytes_reclaimed` | Сколько действительно было освобождено байтов в ходе сканирования в режиме pointer, с учетом замен на pointer и удаление blob с ref_count = 0 |
| `objects_relinked` | Число originals, заменённых pointer objects в этом проходе |
| `errors` | Число ошибок обработки/прохода |
| `mode` | `report_only` или `pointer` |

`bytes_reclaimable` в текущей реализации является оценкой дедупликационной экономии по logical references. После успешного pointer scan значение может не стать нулём; это ограничение текущей модели статистики.

## Установка как службы

Используется `github.com/kardianos/service`, который регистрирует Windows Service или systemd unit. Команда `install` не запускает службу немедленно: запуск выполняется средствами ОС.

Во время установки:

- config path преобразуется в абсолютный;
- executable path сохраняется в service manager;
- служба запускает скрытую команду `service-run`;
- service report сохраняется как `report.json` рядом с config;
- относительные `cache.path` и `logging.file` разрешаются относительно каталога config;
- если S3 health check не завершился за 20 секунд, запуск службы завершается ошибкой;

Executable и config нельзя перемещать после установки без повторной установки службы.

### Windows Service

Рекомендуемое размещение:

```text
C:\Program Files\S3Dedup\s3-dedup.exe
C:\ProgramData\S3Dedup\config.yaml
C:\ProgramData\S3Dedup\state.db
C:\ProgramData\S3Dedup\service.log
C:\ProgramData\S3Dedup\report.json
```

PowerShell от администратора:

```powershell
& "C:\Program Files\S3Dedup\s3-dedup.exe" install `
  --config "C:\ProgramData\S3Dedup\config.yaml"

Start-Service s3-dedup
Get-Service s3-dedup
Get-CimInstance Win32_Service -Filter "Name='s3-dedup'" |
  Select-Object State, StartMode, PathName
```

Остановка и удаление:

```powershell
Stop-Service s3-dedup
"C:\Program Files\S3Dedup\s3-dedup.exe" uninstall
```

### Linux systemd

Точно подходит для Fedora amd64:

```bash
sudo install -m 755 bin/s3-dedup-linux-amd64 /usr/local/bin/s3-dedup
sudo mkdir -p /etc/s3-dedup /var/lib/s3-dedup /var/log/s3-dedup
sudo cp configs/config.yaml /etc/s3-dedup/config.yaml

sudo /usr/local/bin/s3-dedup install \
  --config /etc/s3-dedup/config.yaml
sudo systemctl start s3-dedup
sudo systemctl status s3-dedup --no-pager
systemctl is-enabled s3-dedup
```

Report будет находиться в:

```text
/etc/s3-dedup/report.json
```

Логи:

```bash
sudo cat /etc/s3-dedup/service.log
sudo journalctl -u s3-dedup --no-pager -n 50
```

Остановка и удаление:

```bash
sudo systemctl stop s3-dedup
sudo /usr/local/bin/s3-dedup uninstall
```

## Docker demo с MinIO

```bash
make demo
```

Сценарий:

1. удаляет предыдущие demo containers и volume;
2. запускает MinIO;
3. создаёт bucket `s3-dedup-demo`;
4. загружает `testdata/control` в prefix `input/`;
5. выполняет первый `scan-once`;
6. проверяет результат дедупликации;
7. выполняет второй scan;
8. проверяет идемпотентность;
9. сохраняет reports в предсказуемые файлы.

Ожидаемый контрольный результат, зафиксированный в `scripts/demo.sh`:

| Поле | Первый scan | Второй scan |
|---|---:|---:|
| `objects_scanned` | 120 | 120 |
| `unique_blobs` | 6 | 6 |
| `duplicates_found` | 114 | 114 |
| `objects_relinked` | 120 | 0 |
| `errors` | 0 | 0 |

Файлы результата:

```text
build/demo/report-first.json
build/demo/report-second.json
build/demo/state.db
build/demo/s3-dedup.log
```

## Известные ограничения

- Реализован только SQLite backend; LevelDB отсутствует.
- S3 client создаётся с `Secure: false`, поэтому текущая версия рассчитана на HTTP endpoint.
- Дедупликация выполняется внутри одного bucket: одинаковые данные разных buckets имеют разные blobs.
- Pointer object — пользовательский JSON format; обычный S3 client получает pointer JSON, а не исходное содержимое.
- В `pointer` mode содержимое временно записывается на локальный диск; требуется свободное место не меньше размера одновременно обрабатываемых objects.
- Cache является локальным и не предназначен для общего использования несколькими пользователями.
- `bytes_reclaimable` отражает теоретическое повторяющееся содержимое и может оставаться ненулевым после pointer scan.
- Команда `report` читает только `./report.json`; произвольный input path отдельно не настраивается.

## Документ решения

Архитектура, порядок безопасных операций, refcount, восстановление после сбоев и направления развития описаны в [docs/reshenie.md](docs/reshenie.md).
