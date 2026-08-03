#!/usr/bin/env python3
"""
Мониторинг процесса transferctl или ydb-ch-replicator.

Каждые N секунд печатает:
  - Текущее потребление CPU% и памяти (RSS)
  - Кумулятивные метрики (min / avg / median / max) за всё время наблюдения.
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime

# ── helpers ──────────────────────────────────────────────────────────────

def find_pids(names: list[str]) -> dict[str, list[int]]:
    """Return {name: [pid, ...]} for matching running processes."""
    found: dict[str, list[int]] = defaultdict(list)
    try:
        out = subprocess.check_output(
            ["ps", "-eo", "pid,comm", "-o", "args"],
            text=True, timeout=5,
        )
    except subprocess.CalledProcessError:
        return found

    for line in out.splitlines():
        parts = line.split(maxsplit=2)
        if len(parts) < 3:
            continue
        pid_str, comm, args = parts
        try:
            pid = int(pid_str)
        except ValueError:
            continue
        for name in names:
            # match either the short comm or the full command line
            if name in comm or name in args:
                found[name].append(pid)
    return found


def ps_snapshot(pids: list[int]) -> list[dict]:
    """
    Run `ps` once for the given pids and return a list of dicts with:
        pid, cpu_percent, rss_mb, vsz_mb, elapsed
    """
    if not pids:
        return []
    pid_list = ",".join(str(p) for p in pids)
    try:
        out = subprocess.check_output(
            ["ps", "-p", pid_list, "-o", "pid=,pcpu=,rss=,vsz=,etime="],
            text=True, timeout=5,
        )
    except subprocess.CalledProcessError:
        return []

    snaps = []
    for line in out.strip().splitlines():
        parts = line.split()
        if len(parts) < 5:
            continue
        try:
            pid = int(parts[0])
            cpu = float(parts[1])
            rss_kb = int(parts[2])
            vsz_kb = int(parts[3])
            elapsed = parts[4]  # e.g. "01:23" or "12:34:56"
        except (ValueError, IndexError):
            continue
        snaps.append({
            "pid": pid,
            "cpu_percent": cpu,
            "rss_mb": rss_kb / 1024,
            "vsz_mb": vsz_kb / 1024,
            "elapsed": elapsed,
        })
    return snaps


def parse_elapsed(elapsed: str) -> int:
    """Convert `ps` etime string (dd-hh:mm:ss or hh:mm:ss or mm:ss) to seconds."""
    parts = elapsed.split("-")
    days = int(parts[0]) if len(parts) == 2 else 0
    rest = parts[-1]
    h, m, s = rest.split(":")
    return days * 86400 + int(h) * 3600 + int(m) * 60 + int(s)


def percentile(sorted_vals: list[float], p: float) -> float:
    """p in 0..100."""
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p / 100
    f = int(k)
    c = k - f
    if f + 1 < len(sorted_vals):
        return sorted_vals[f] + c * (sorted_vals[f + 1] - sorted_vals[f])
    return sorted_vals[f]


# ── main ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Мониторинг transferctl / ydb-ch-replicator"
    )
    parser.add_argument(
        "-i", "--interval", type=float, default=3.0,
        help="Интервал опроса в секундах (по умолчанию 3)",
    )
    parser.add_argument(
        "-n", "--count", type=int, default=0,
        help="Количество замеров (0 = бесконечно)",
    )
    parser.add_argument(
        "--names", nargs="+", default=["transferctl", "ydb-ch-replicator"],
        help="Имена процессов для поиска",
    )
    parser.add_argument(
        "--dump", type=str, default="",
        help="Сохранить CSV-лог в указанный файл",
    )
    args = parser.parse_args()

    print(f"🔍 Ищу процессы: {args.names}")
    found = find_pids(args.names)
    if not found:
        print("❌ Ни один из процессов не найден.")
        sys.exit(1)

    all_pids: list[int] = []
    for name, pids in found.items():
        print(f"   ✓ {name}: pid {pids}")
        all_pids.extend(pids)

    csv_file = None
    if args.dump:
        csv_file = open(args.dump, "w")
        csv_file.write("timestamp,pid,cpu%,rss_mb,vsz_mb,elapsed_sec\n")

    # history
    cpu_history: list[float] = []
    rss_history: list[float] = []

    print()
    header = (
        f"{'Time':>12s}  {'PID':>6s}  {'CPU%':>7s}  {'RSS_MB':>8s}  {'VSZ_MB':>8s}  "
        f"{'Elapsed':>10s}  │  "
        f"{'CPU(min/avg/med/max)':>28s}  {'RSS(min/avg/med/max)':>28s}"
    )
    print(header)
    print("-" * len(header))

    iteration = 0
    try:
        while True:
            snaps = ps_snapshot(all_pids)
            now = datetime.now().strftime("%H:%M:%S")

            if not snaps:
                print(f"[{now}] ⚠️  процесс(ы) исчезли — жду...")
                time.sleep(args.interval)
                # re-discover
                found = find_pids(args.names)
                all_pids = []
                for name, pids in found.items():
                    all_pids.extend(pids)
                if not all_pids:
                    print("❌ Процессы не найдены. Выход.")
                    break
                continue

            for s in snaps:
                cpu_history.append(s["cpu_percent"])
                rss_history.append(s["rss_mb"])

                # cumulative stats per-pid
                # But cpu_history is across all pids — close enough for a monitor
                cpu_sorted = sorted(cpu_history)
                rss_sorted = sorted(rss_history)

                cpu_stats = (
                    f"{min(cpu_history):5.1f} / {sum(cpu_history)/len(cpu_history):5.1f} / "
                    f"{percentile(cpu_sorted, 50):5.1f} / {max(cpu_history):5.1f}"
                )
                rss_stats = (
                    f"{min(rss_history):5.1f} / {sum(rss_history)/len(rss_history):5.1f} / "
                    f"{percentile(rss_sorted, 50):5.1f} / {max(rss_history):5.1f}"
                )

                elapsed_sec = parse_elapsed(s["elapsed"])

                print(
                    f"{now:>12s}  {s['pid']:>6d}  {s['cpu_percent']:>6.1f}%  "
                    f"{s['rss_mb']:>7.1f}M  {s['vsz_mb']:>7.1f}M  "
                    f"{elapsed_sec:>8d}s  │  "
                    f"{cpu_stats:>28s}  {rss_stats:>28s}"
                )

                if csv_file:
                    csv_file.write(
                        f"{now},{s['pid']},{s['cpu_percent']:.1f},"
                        f"{s['rss_mb']:.1f},{s['vsz_mb']:.1f},{elapsed_sec}\n"
                    )

            iteration += 1
            if args.count and iteration >= args.count:
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n👋 Остановлено.")

    finally:
        if csv_file:
            csv_file.close()
            print(f"📄 CSV сохранён: {args.dump}")


if __name__ == "__main__":
    main()
