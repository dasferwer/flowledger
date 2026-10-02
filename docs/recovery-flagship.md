# Потерян replication slot: как пересобирается проекция

Инвариант: после завершения пересборки текущая проекция двух таблиц совпадает
с источником по ключам и всем полям. Старые сообщения не могут записаться в новую
generation. Потеря slot означает потерю доступной истории изменений и требует
нового согласованного снимка, а не продолжения со старого LSN.

Отдельное имя проекта изолирует контейнеры и тома; дополнительный Compose-файл
меняет host-порты и добавляет ожидание первого готового снимка через healthcheck.
Порты 54840, 54940 и 5640 должны быть свободны:

```bash
export COMPOSE_PROJECT_NAME=proof-flowledger-demo
export COMPOSE_FILE=compose.yaml:compose.proof.yaml
export SOURCE_URL=postgresql://flowledger_cdc:cdc-demo@localhost:54840/source
export SOURCE_BOOTSTRAP_URL=postgresql://flowledger_owner:owner-demo@localhost:54840/source
export TARGET_URL=postgresql://demo:demo@localhost:54940/projection
export AMQP_URL=amqp://demo:demo@localhost:5640/%2F
uv sync --frozen
docker compose up --build -d --wait --wait-timeout 180
uv run python -m scripts.recovery
docker compose down -v
```

Нужны Docker Compose, Python 3.11+ и uv. Сценарий меняет демонстрационные данные
и останавливает компоненты указанного Compose-проекта. Используйте отдельный
стенд. Последняя команда удаляет только его данные. Compose должен поддерживать
`!override`; корректность конфигурации проверяется через `docker compose config --quiet`.

[Recovery](../scripts/recovery.py) сначала проверяет обычный поток и перезапуски.
Затем останавливает publisher, удаляет его slot и меняет источник. После запуска
publisher должна смениться generation; скрипт ждёт `ready` и полной сверки
источника с проекцией. Проверяются не только количество строк, но и все поля
`items` и `stores`. Дополнительная граница — изменение схемы таблицы.

Runtime publisher использует ограниченную роль `flowledger_cdc`, а подготовка
данных — отдельного владельца `flowledger_owner`. Сценарий не требует superuser
для снимка и пересборки. Он входит в CI вместе с проверками границ отказа.

Для интервью покажите исчезнувший slot, новую generation и полную сверку.
Восстановление возвращает текущее состояние источника; события, которые больше
нельзя прочитать из WAL, оно не восстанавливает как историю. Это не проверка
failover PostgreSQL или физической потери брокера; WAL требует отдельного
операторского бюджета хранения. Условия и результаты: [VERIFICATION.md](../VERIFICATION.md).
Свежий локальный результат этапа D: [verification-stage-d.json](verification-stage-d.json).
