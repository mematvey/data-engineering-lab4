# data-engineering-lab4

Домашнее задание 4 по инженерии данных, Apache ZooKeeper, вариант 9 (разработчик на мобильных платформах)
Ансамбль из трёх серверов ZooKeeper 3.9.6 в Docker и координатор расчёта пар навыков из ДЗ 3: выбор лидера, чекпоинты, приостановка лидера и защита от устаревших записей по эпохе
Отчёт лежит в docs/report.pdf, журналы запусков в logs, выводы команд в outputs, настройки подключения в .env (шаблон .env.example)
Нужен RustFS из ДЗ 3 (rustfs/docker-compose.yml) с бакетом hh-lake: в нём файл raw/it_vacancies_2024.csv и результат MapReduce results/mapreduce/stack_2.csv, с которым сверяется результат координатора

```
docker compose --env-file ..\.env -f rustfs\docker-compose.yml up -d
docker compose up -d
uv sync
powershell scripts\ensemble.ps1
uv run python tools\ephemeral_demo.py
uv run python src\run_scenario.py --run nofence --fencing 0
uv run python src\verify.py --run nofence
uv run python src\run_scenario.py --run fence --fencing 1
uv run python src\verify.py --run fence
```
