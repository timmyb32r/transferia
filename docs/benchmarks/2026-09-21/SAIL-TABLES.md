# Sail: подробные результаты отдельного блока

Медианы успешных повторов. 1M — три повтора; 10M/P4 — один exploratory-прогон. Fresh control — Rust/Spark, запущенные вместе с Sail, а не старые медианы. CPU всех клиентских процессов, включая прокси, учтён; CPU серверов БД не входит.

| Путь | Приёмник | Данные | Части | n | Строк/с | Строк/CPU-с | Средние CPU | Peak RSS GiB | Cgroup peak GiB |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| Rust Auto · fresh control | pg-ch | narrow | 4 | 3 | 527,143 | 482,994 | 1.09 | 0.49 | 0.55 |
| Sail ADBC buffered + sink | pg-ch | narrow | 1 | 3 | 266,422 | 209,106 | 1.27 | 0.42 | 0.37 |
| Sail ADBC buffered + sink | pg-ch | narrow | 4 | 3 | 408,868 | 178,295 | 2.30 | 0.51 | 0.41 |
| Sail JDBC/ConnectorX + sink | pg-ch | narrow | 1 | 3 | 292,182 | 212,123 | 1.37 | 0.38 | 0.27 |
| Sail JDBC/ConnectorX + sink | pg-ch | narrow | 4 | 3 | 498,488 | 192,460 | 2.53 | 0.43 | 0.33 |
| Spark · fresh control | pg-ch | narrow | 4 | 3 | 96,005 | 35,522 | 2.70 | 0.85 | 0.84 |
| Rust Auto · fresh control | pg-ch | narrow10m | 4 | 1 | 1,342,705 | 516,662 | 2.60 | 1.28 | 1.63 |
| Sail ADBC buffered + sink | pg-ch | narrow10m | 4 | 1 | 1,547,492 | 400,888 | 3.86 | 2.16 | 2.16 |
| Spark · fresh control | pg-ch | narrow10m | 4 | 1 | 466,706 | 223,755 | 2.09 | 1.14 | 1.13 |
| Rust Auto · fresh control | pg-ch | wide | 4 | 3 | 323,562 | 155,286 | 2.07 | 1.20 | 1.41 |
| Sail ADBC buffered + sink | pg-ch | wide | 1 | 3 | 122,751 | 74,407 | 1.65 | 1.96 | 2.10 |
| Sail ADBC buffered + sink | pg-ch | wide | 4 | 3 | 239,857 | 68,396 | 3.61 | 1.56 | 1.57 |
| Sail JDBC/ConnectorX + sink | pg-ch | wide | 1 | 3 | 132,919 | 72,218 | 1.84 | 1.25 | 1.14 |
| Sail JDBC/ConnectorX + sink | pg-ch | wide | 4 | 3 | 283,187 | 67,345 | 4.23 | 1.29 | 1.19 |
| Spark · fresh control | pg-ch | wide | 4 | 3 | 63,277 | 25,096 | 2.50 | 3.95 | 3.94 |
| Rust Auto · fresh control | pg-pg | narrow | 4 | 3 | 392,957 | 441,772 | 0.89 | 0.69 | 0.72 |
| Sail ADBC buffered + sink | pg-pg | narrow | 1 | 3 | 196,396 | 191,820 | 1.04 | 0.52 | 0.41 |
| Sail ADBC buffered + sink | pg-pg | narrow | 4 | 3 | 303,179 | 162,936 | 1.77 | 0.61 | 0.49 |
| Sail JDBC/ConnectorX + sink | pg-pg | narrow | 1 | 3 | 246,750 | 198,357 | 1.24 | 0.38 | 0.26 |
| Sail JDBC/ConnectorX + sink | pg-pg | narrow | 4 | 3 | 350,362 | 170,075 | 2.07 | 0.45 | 0.34 |
| Spark · fresh control | pg-pg | narrow | 4 | 3 | 80,751 | 35,521 | 2.27 | 0.83 | 0.81 |
| Rust Auto · fresh control | pg-pg | narrow10m | 4 | 1 | 645,085 | 494,451 | 1.30 | 1.99 | 2.31 |
| Sail ADBC buffered + sink | pg-pg | narrow10m | 4 | 1 | 682,656 | 337,199 | 2.02 | 3.29 | 3.17 |
| Spark · fresh control | pg-pg | narrow10m | 4 | 1 | 218,169 | 198,770 | 1.10 | 0.85 | 0.83 |
| Rust Auto · fresh control | pg-pg | wide | 4 | 3 | 173,428 | 137,004 | 1.27 | 2.61 | 2.76 |
| Sail ADBC buffered + sink | pg-pg | wide | 1 | 3 | 88,336 | 65,029 | 1.35 | 2.29 | 2.18 |
| Sail ADBC buffered + sink | pg-pg | wide | 4 | 3 | 143,556 | 58,145 | 2.43 | 2.38 | 2.27 |
| Sail JDBC/ConnectorX + sink | pg-pg | wide | 1 | 3 | 102,304 | 66,600 | 1.54 | 1.31 | 1.19 |
| Sail JDBC/ConnectorX + sink | pg-pg | wide | 4 | 3 | 166,999 | 60,506 | 2.88 | 1.57 | 1.45 |
| Spark · fresh control | pg-pg | wide | 4 | 3 | 64,936 | 28,216 | 2.26 | 1.51 | 1.48 |

## Покрытие

Успешно 78 из 80 запущенных. План: 48 Sail 1M + 4 Sail 10M + 24 контрольных 1M + 4 контрольных 10M = 80. Отсутствующие/неудачные случаи не имеют значения throughput.

- sail_jdbc_narrow10m_p4_r0_1790000266: configured run deadline exceeded (600.1 с)
- sail_jdbc_narrow10m_p4_r0_1790000870_ch: configured run deadline exceeded (600.1 с)
