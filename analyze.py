#!/usr/bin/env python3
"""
analyze.py - Genera gráficos y tablas para el informe técnico.
Lee los CSVs de la carpeta results/ y produce figuras en results/figures/.

Métricas analizadas según la tarea:
  - Hit rate
  - Throughput (consultas/segundo)
  - Latencia p50/p95
  - Eviction rate
  - Cache efficiency
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path

RESULTS_DIR = Path("results")
FIGURES_DIR = RESULTS_DIR / "figures"
EVENTS_FILE = RESULTS_DIR / "events.csv"
EXPERIMENTS_FILE = RESULTS_DIR / "experiments_summary.csv"


def setup_style():
    plt.rcParams.update({
        "figure.dpi": 150,
        "font.family": "DejaVu Sans",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.grid": True,
        "grid.alpha": 0.3,
        "figure.facecolor": "white",
    })


def plot_hit_rate_comparison(df: pd.DataFrame, group_col: str, title: str, filename: str):
    """Gráfico de barras comparando hit rate entre grupos."""
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle(title, fontsize=14, fontweight="bold")

    metrics = ["hit_rate", "latency_p50_ms", "latency_p95_ms"]
    labels = ["Hit Rate (%)", "Latencia p50 (ms)", "Latencia p95 (ms)"]
    colors = ["#2196F3", "#4CAF50", "#FF9800"]

    for ax, metric, label, color in zip(axes, metrics, labels, colors):
        groups = df[group_col].tolist()
        values = df[metric].tolist()
        bars = ax.bar(groups, values, color=color, alpha=0.85, edgecolor="white", linewidth=1.5)
        ax.set_title(label, fontsize=11)
        ax.set_ylabel(label)
        ax.set_xlabel(group_col.replace("_", " ").title())
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    f"{val:.1f}", ha="center", va="bottom", fontsize=9, fontweight="bold")

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / filename, bbox_inches="tight")
    plt.close()
    print(f"  Figura guardada: {filename}")


def plot_latency_distribution(events_df: pd.DataFrame):
    """Histograma de distribución de latencias HIT vs MISS."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Distribución de Latencias: HIT vs MISS", fontsize=14, fontweight="bold")

    hits = events_df[events_df["event_type"] == "hit"]["latency_ms"]
    misses = events_df[events_df["event_type"] == "miss"]["latency_ms"]

    ax1.hist(hits, bins=30, color="#4CAF50", alpha=0.8, edgecolor="white")
    ax1.set_title(f"Cache HITs (n={len(hits):,})")
    ax1.set_xlabel("Latencia (ms)")
    ax1.set_ylabel("Frecuencia")
    ax1.axvline(hits.median(), color="darkgreen", linestyle="--", label=f"Mediana: {hits.median():.1f}ms")
    ax1.legend()

    ax2.hist(misses, bins=30, color="#F44336", alpha=0.8, edgecolor="white")
    ax2.set_title(f"Cache MISSes (n={len(misses):,})")
    ax2.set_xlabel("Latencia (ms)")
    ax2.set_ylabel("Frecuencia")
    ax2.axvline(misses.median(), color="darkred", linestyle="--", label=f"Mediana: {misses.median():.1f}ms")
    ax2.legend()

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "latency_distribution.png", bbox_inches="tight")
    plt.close()
    print("  Figura guardada: latency_distribution.png")


def plot_throughput_over_time(events_df: pd.DataFrame):
    """Throughput agregado por segundo a lo largo del tiempo."""
    events_df = events_df.copy()
    events_df["second"] = (events_df["timestamp"] - events_df["timestamp"].min()).astype(int)
    throughput = events_df.groupby("second").size()

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.fill_between(throughput.index, throughput.values, alpha=0.3, color="#2196F3")
    ax.plot(throughput.index, throughput.values, color="#2196F3", linewidth=2)
    ax.set_title("Throughput del Sistema a lo Largo del Tiempo", fontsize=14, fontweight="bold")
    ax.set_xlabel("Tiempo (segundos)")
    ax.set_ylabel("Consultas por segundo")
    ax.axhline(throughput.mean(), color="red", linestyle="--", label=f"Promedio: {throughput.mean():.1f} req/s")
    ax.legend()

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "throughput_timeline.png", bbox_inches="tight")
    plt.close()
    print("  Figura guardada: throughput_timeline.png")


def plot_hit_rate_by_zone(events_df: pd.DataFrame):
    """Hit rate por zona geográfica."""
    zone_stats = events_df.groupby(["zone_id", "event_type"]).size().unstack(fill_value=0)
    if "hit" not in zone_stats.columns:
        zone_stats["hit"] = 0
    if "miss" not in zone_stats.columns:
        zone_stats["miss"] = 0
    zone_stats["hit_rate"] = zone_stats["hit"] / (zone_stats["hit"] + zone_stats["miss"]) * 100
    zone_stats["total"] = zone_stats["hit"] + zone_stats["miss"]
    zone_stats = zone_stats.sort_index()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Análisis por Zona Geográfica", fontsize=14, fontweight="bold")

    colors = ["#2196F3", "#4CAF50", "#FF9800", "#9C27B0", "#F44336"]
    ax1.bar(zone_stats.index, zone_stats["hit_rate"], color=colors[:len(zone_stats)], alpha=0.85, edgecolor="white")
    ax1.set_title("Hit Rate por Zona (%)")
    ax1.set_xlabel("Zona")
    ax1.set_ylabel("Hit Rate (%)")
    ax1.set_ylim(0, 100)
    for i, (zone, row) in enumerate(zone_stats.iterrows()):
        ax1.text(i, row["hit_rate"] + 1, f"{row['hit_rate']:.1f}%", ha="center", fontsize=9, fontweight="bold")

    ax2.bar(zone_stats.index, zone_stats["total"], color=colors[:len(zone_stats)], alpha=0.85, edgecolor="white")
    ax2.set_title("Total de Consultas por Zona (distribución de tráfico)")
    ax2.set_xlabel("Zona")
    ax2.set_ylabel("Total consultas")
    for i, (zone, row) in enumerate(zone_stats.iterrows()):
        ax2.text(i, row["total"] + 1, f"{int(row['total'])}", ha="center", fontsize=9, fontweight="bold")

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "hit_rate_by_zone.png", bbox_inches="tight")
    plt.close()
    print("  Figura guardada: hit_rate_by_zone.png")


def generate_summary_table(exp_df: pd.DataFrame):
    """Genera una tabla resumen en markdown para el informe."""
    table_path = RESULTS_DIR / "summary_table.md"
    with open(table_path, "w") as f:
        f.write("# Resumen de Experimentos\n\n")

        for experiment in exp_df["experiment"].unique():
            subset = exp_df[exp_df["experiment"] == experiment].copy()
            f.write(f"## {experiment.replace('_', ' ').title()}\n\n")
            f.write("| Configuración | Hit Rate (%) | Latencia p50 (ms) | Latencia p95 (ms) | Evictions |\n")
            f.write("|---|---|---|---|---|\n")
            for _, row in subset.iterrows():
                label = row.get("distribution") or row.get("eviction_policy") or row.get("maxmemory") or str(row.get("ttl")) + "s"
                f.write(f"| {label} | {row['hit_rate']:.1f} | {row['latency_p50_ms']:.1f} | {row['latency_p95_ms']:.1f} | {row['evicted_keys']} |\n")
            f.write("\n")

    print(f"  Tabla resumen guardada en {table_path}")


def main():
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    setup_style()

    print("Generando gráficos y tablas de análisis...\n")

    # Cargar events si existe
    if EVENTS_FILE.exists():
        events_df = pd.read_csv(EVENTS_FILE)
        print(f"Cargando {len(events_df):,} eventos desde {EVENTS_FILE}")
        plot_latency_distribution(events_df)
        plot_throughput_over_time(events_df)
        plot_hit_rate_by_zone(events_df)
    else:
        print(f"ADVERTENCIA: {EVENTS_FILE} no existe. Copia el CSV desde el contenedor primero.")
        print("  Ejecuta: docker cp metrics_storage:/metrics/events.csv results/events.csv")

    # Cargar experimentos si existe
    if EXPERIMENTS_FILE.exists():
        exp_df = pd.read_csv(EXPERIMENTS_FILE)
        print(f"Cargando {len(exp_df)} experimentos desde {EXPERIMENTS_FILE}")

        # Experimento 1: Distribuciones
        dist_df = exp_df[exp_df["experiment"] == "dist_comparison"]
        if not dist_df.empty:
            plot_hit_rate_comparison(dist_df, "distribution", "Comparación: Distribución de Tráfico", "dist_comparison.png")

        # Experimento 2: Políticas
        policy_df = exp_df[exp_df["experiment"] == "policy_comparison"]
        if not policy_df.empty:
            plot_hit_rate_comparison(policy_df, "eviction_policy", "Comparación: Políticas de Evicción", "policy_comparison.png")

        # Experimento 3: Tamaño
        size_df = exp_df[exp_df["experiment"] == "size_comparison"]
        if not size_df.empty:
            plot_hit_rate_comparison(size_df, "maxmemory", "Comparación: Tamaño de Caché", "size_comparison.png")

        # Experimento 4: TTL
        ttl_df = exp_df[exp_df["experiment"] == "ttl_comparison"]
        if not ttl_df.empty:
            ttl_df = ttl_df.copy()
            ttl_df["ttl"] = ttl_df["ttl"].astype(str) + "s"
            plot_hit_rate_comparison(ttl_df, "ttl", "Comparación: TTL", "ttl_comparison.png")

        generate_summary_table(exp_df)
    else:
        print(f"ADVERTENCIA: {EXPERIMENTS_FILE} no existe. Ejecuta run_experiments.py primero.")

    print(f"\n[DONE] Figuras generadas en {FIGURES_DIR}/")


if __name__ == "__main__":
    main()
