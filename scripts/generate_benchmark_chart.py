#!/usr/bin/env python3
"""
Generate benchmark performance charts from Criterion output.

This script reads Criterion benchmark results and generates a comprehensive
chart showing throughput (Kelem/s) and processing time (ms) across different
batch sizes and configurations.
"""

import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path
import sys
import numpy as np

# Benchmark name mappings to readable labels
BENCHMARK_LABELS = {
    "bench_fulls_one_large_table_insert_only": "1 table\n100% inserts",
    "bench_fulls_one_large_table_update_only": "1 table\n100% updates",
    "bench_fulls_one_large_table_delete_only": "1 table\n100% deletes",
    "bench_fulls_one_large_table_mixed": "1 table\n40% inserts\n50% updates\n10% deletes",
    "bench_fulls_four_large_tables_mixed": "4 tables\n40% inserts\n50% updates\n10% deletes",
    "bench_partials": "Partial changesets\n(buffered)",
}

# Batch sizes to display (columns in the chart)
BATCH_SIZES = [1, 2, 5, 10, 25, 50, 100]
DEFAULT_BATCH_SIZE = 100

def parse_criterion_results(target_dir: Path):
    """
    Parse Criterion benchmark results from the target directory.
    
    Returns a dict mapping benchmark_name -> batch_size -> {"throughput": kelem_per_sec, "elements": num_elements}
    """
    results = {}
    
    for bench_name in BENCHMARK_LABELS.keys():
        bench_dir = target_dir / bench_name
        if not bench_dir.exists():
            print(f"Warning: Benchmark directory not found: {bench_dir}", file=sys.stderr)
            continue
            
        results[bench_name] = {}
        
        for batch_size in BATCH_SIZES:
            benchmark_file = bench_dir / str(batch_size) / "new" / "benchmark.json"
            estimates_file = bench_dir / str(batch_size) / "new" / "estimates.json"
            
            if not benchmark_file.exists() or not estimates_file.exists():
                print(f"Warning: Results not found for {bench_name}/{batch_size}", file=sys.stderr)
                continue
            
            try:
                # Read throughput (number of elements) from benchmark.json
                with open(benchmark_file) as f:
                    benchmark_data = json.load(f)
                
                throughput_info = benchmark_data.get("throughput", {})
                num_elements = throughput_info.get("Elements", 0)
                
                if num_elements == 0:
                    print(f"Warning: No throughput data for {bench_name}/{batch_size}", file=sys.stderr)
                    continue
                
                # Read mean time from estimates.json
                with open(estimates_file) as f:
                    estimates_data = json.load(f)
                
                mean_time_ns = estimates_data.get("mean", {}).get("point_estimate", 0)
                
                if mean_time_ns == 0:
                    print(f"Warning: No timing data for {bench_name}/{batch_size}", file=sys.stderr)
                    continue
                
                # Calculate throughput: elements per second
                # mean_time_ns is in nanoseconds per iteration
                # 1 second = 1e9 nanoseconds
                elems_per_sec = (num_elements / mean_time_ns) * 1e9
                kelem_per_sec = elems_per_sec / 1000.0
                
                # Convert time to milliseconds
                time_ms = mean_time_ns / 1e6
                
                results[bench_name][batch_size] = {
                    "throughput": kelem_per_sec,
                    "elements": num_elements,
                    "time_ms": time_ms
                }
                        
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing {benchmark_file} or {estimates_file}: {e}", file=sys.stderr)
                continue
    
    return results


def generate_chart(results: dict, output_file: Path):
    """
    Generate a comprehensive chart showing all benchmark results.
    """
    # Set up the figure with a clean, professional style
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(18, 12))
    
    # Prepare data for plotting
    benchmark_names = list(BENCHMARK_LABELS.keys())
    
    # Separate benchmarks into pure and mixed
    pure_benchmarks = [
        "bench_fulls_one_large_table_insert_only",
        "bench_fulls_one_large_table_update_only",
        "bench_fulls_one_large_table_delete_only"
    ]
    mixed_benchmarks = [
        "bench_fulls_one_large_table_mixed",
        "bench_fulls_four_large_tables_mixed"
    ]
    
    # Color palette - distinct colors for each benchmark
    pure_colors = ['#2E86AB', '#A23B72', '#F18F01']
    mixed_colors = ['#C73E1D', '#6A994E']
    pure_markers = ['o', 's', '^']
    mixed_markers = ['D', 'v']
    
    # Plot pure operations (left column)
    for idx, bench_name in enumerate(pure_benchmarks):
        if bench_name not in results:
            continue
        
        # Get x values (number of elements) and y values (throughput and time)
        x_values = []
        throughput_values = []
        time_values = []
        
        for batch_size in BATCH_SIZES:
            if batch_size in results[bench_name]:
                data = results[bench_name][batch_size]
                x_values.append(data["elements"])
                throughput_values.append(data["throughput"])
                time_values.append(data["time_ms"])
        
        if not x_values:
            continue
        
        # Plot throughput (top left)
        ax1.plot(
            x_values,
            throughput_values,
            label=BENCHMARK_LABELS[bench_name],
            color=pure_colors[idx],
            marker=pure_markers[idx],
            markersize=8,
            linewidth=2.5,
            alpha=0.9
        )
        
        # Plot processing time (bottom left)
        ax3.plot(
            x_values,
            time_values,
            label=BENCHMARK_LABELS[bench_name],
            color=pure_colors[idx],
            marker=pure_markers[idx],
            markersize=8,
            linewidth=2.5,
            alpha=0.9
        )
    
    # Plot mixed operations (right column)
    for idx, bench_name in enumerate(mixed_benchmarks):
        if bench_name not in results:
            continue
        
        # Get x values (number of elements) and y values (throughput and time)
        x_values = []
        throughput_values = []
        time_values = []
        
        for batch_size in BATCH_SIZES:
            if batch_size in results[bench_name]:
                data = results[bench_name][batch_size]
                x_values.append(data["elements"])
                throughput_values.append(data["throughput"])
                time_values.append(data["time_ms"])
        
        if not x_values:
            continue
        
        # Plot throughput (top right)
        ax2.plot(
            x_values,
            throughput_values,
            label=BENCHMARK_LABELS[bench_name],
            color=mixed_colors[idx],
            marker=mixed_markers[idx],
            markersize=8,
            linewidth=2.5,
            alpha=0.9
        )
        
        # Plot processing time (bottom right)
        ax4.plot(
            x_values,
            time_values,
            label=BENCHMARK_LABELS[bench_name],
            color=mixed_colors[idx],
            marker=mixed_markers[idx],
            markersize=8,
            linewidth=2.5,
            alpha=0.9
        )
    
    # Add vertical line at the default batch size on all subplots
    for ax in [ax1, ax2, ax3, ax4]:
        ax.axvline(x=DEFAULT_BATCH_SIZE, color='red', linestyle='--', linewidth=2, alpha=0.7, zorder=1)
    
    # Add labels on the top subplots
    ax1.text(DEFAULT_BATCH_SIZE, ax1.get_ylim()[1] * 0.95, 'Default batch size', 
            rotation=90, verticalalignment='top', horizontalalignment='right',
            fontsize=9, color='red', fontweight='bold')
    ax2.text(DEFAULT_BATCH_SIZE, ax2.get_ylim()[1] * 0.95, 'Default batch size', 
            rotation=90, verticalalignment='top', horizontalalignment='right',
            fontsize=9, color='red', fontweight='bold')
    
    # Customize top left (pure throughput)
    ax1.set_ylabel('Throughput (Kelem/s)', fontsize=11, fontweight='bold')
    ax1.set_title('Pure Operations', fontsize=12, fontweight='bold', pad=15)
    ax1.legend(loc='upper left', framealpha=0.9, fontsize=8)
    ax1.grid(True, alpha=0.6, linestyle='-', linewidth=0.8, which='major', color='gray')
    ax1.grid(True, alpha=0.3, linestyle=':', linewidth=0.5, which='minor', color='gray')
    ax1.set_xscale('log')
    ax1.set_facecolor('white')
    
    # Customize top right (mixed throughput)
    ax2.set_title('Mixed Operations', fontsize=12, fontweight='bold', pad=15)
    ax2.legend(loc='upper left', framealpha=0.9, fontsize=8)
    ax2.grid(True, alpha=0.6, linestyle='-', linewidth=0.8, which='major', color='gray')
    ax2.grid(True, alpha=0.3, linestyle=':', linewidth=0.5, which='minor', color='gray')
    ax2.set_xscale('log')
    ax2.set_facecolor('white')
    
    # Align Y-axis for top row (throughput)
    y1_min, y1_max = ax1.get_ylim()
    y2_min, y2_max = ax2.get_ylim()
    y_min = min(y1_min, y2_min)
    y_max = max(y1_max, y2_max)
    ax1.set_ylim(y_min, y_max)
    ax2.set_ylim(y_min, y_max)
    
    # Customize bottom left (pure processing time)
    ax3.set_xlabel('Number of Changes', fontsize=11, fontweight='bold')
    ax3.set_ylabel('Processing Time (ms)', fontsize=11, fontweight='bold')
    ax3.legend(loc='upper left', framealpha=0.9, fontsize=8)
    ax3.grid(True, alpha=0.6, linestyle='-', linewidth=0.8, which='major', color='gray')
    ax3.grid(True, alpha=0.3, linestyle=':', linewidth=0.5, which='minor', color='gray')
    ax3.set_xscale('log')
    ax3.set_yscale('log', base=2)
    ax3.set_facecolor('white')
    
    # Customize bottom right (mixed processing time)
    ax4.set_xlabel('Number of Changes', fontsize=11, fontweight='bold')
    ax4.legend(loc='upper left', framealpha=0.9, fontsize=8)
    ax4.grid(True, alpha=0.6, linestyle='-', linewidth=0.8, which='major', color='gray')
    ax4.grid(True, alpha=0.3, linestyle=':', linewidth=0.5, which='minor', color='gray')
    ax4.set_xscale('log')
    ax4.set_yscale('log', base=2)
    ax4.set_facecolor('white')
    
    # Align Y-axis for bottom row (processing time)
    y3_min, y3_max = ax3.get_ylim()
    y4_min, y4_max = ax4.get_ylim()
    y_min = min(y3_min, y4_min)
    y_max = max(y3_max, y4_max)
    ax3.set_ylim(y_min, y_max)
    ax4.set_ylim(y_min, y_max)
    
    # Add overall title
    fig.suptitle('Changes Sync Performance', 
                 fontsize=14, fontweight='bold', y=0.995)
    
    # Add a subtle background
    fig.patch.set_facecolor('white')
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout(rect=[0, 0, 1, 0.99])
    
    # Save the chart
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Chart saved to: {output_file}")
    
    # Also save as SVG for better quality in presentations
    svg_file = output_file.with_suffix('.svg')
    plt.savefig(svg_file, format='svg', bbox_inches='tight')
    print(f"SVG chart saved to: {svg_file}")
    
    plt.close()


def generate_partials_chart(results: dict, output_file: Path):
    """
    Generate a separate chart for partial changesets benchmark.
    """
    if "bench_partials" not in results:
        print("Warning: No bench_partials results found, skipping partials chart")
        return
    
    # Set up the figure with a clean, professional style
    plt.style.use('seaborn-v0_8-darkgrid')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Get data for bench_partials
    bench_name = "bench_partials"
    x_values = []
    throughput_values = []
    time_values = []
    
    for batch_size in BATCH_SIZES:
        if batch_size in results[bench_name]:
            data = results[bench_name][batch_size]
            x_values.append(data["elements"])
            throughput_values.append(data["throughput"])
            time_values.append(data["time_ms"])
    
    if not x_values:
        print("Warning: No data points for bench_partials")
        return
    
    # Color for partials
    color = '#9B59B6'
    marker = 'D'
    
    # Plot throughput (left)
    ax1.plot(
        x_values,
        throughput_values,
        label=BENCHMARK_LABELS[bench_name],
        color=color,
        marker=marker,
        markersize=8,
        linewidth=2.5,
        alpha=0.9
    )
    
    # Plot processing time (right)
    ax2.plot(
        x_values,
        time_values,
        label=BENCHMARK_LABELS[bench_name],
        color=color,
        marker=marker,
        markersize=8,
        linewidth=2.5,
        alpha=0.9
    )
    
    # Add vertical line at x=DEFAULT_BATCH_SIZE on both subplots
    for ax in [ax1, ax2]:
        ax.axvline(x=DEFAULT_BATCH_SIZE, color='red', linestyle='--', linewidth=2, alpha=0.7, zorder=1)
    
    # Add label on the left subplot
    ax1.text(DEFAULT_BATCH_SIZE, ax1.get_ylim()[1] * 0.95, 'Default batch size', 
            rotation=90, verticalalignment='top', horizontalalignment='right',
            fontsize=9, color='red', fontweight='bold')
    
    # Customize left subplot (throughput)
    ax1.set_xlabel('Number of Changes', fontsize=11, fontweight='bold')
    ax1.set_ylabel('Throughput (Kelem/s)', fontsize=11, fontweight='bold')
    ax1.set_title('Throughput', fontsize=12, fontweight='bold', pad=15)
    ax1.legend(loc='upper left', framealpha=0.9, fontsize=9)
    ax1.grid(True, alpha=0.6, linestyle='-', linewidth=0.8, which='major', color='gray')
    ax1.grid(True, alpha=0.3, linestyle=':', linewidth=0.5, which='minor', color='gray')
    ax1.set_xscale('log')
    ax1.set_facecolor('white')
    
    # Customize right subplot (processing time)
    ax2.set_xlabel('Number of Changes', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Processing Time (ms)', fontsize=11, fontweight='bold')
    ax2.set_title('Processing Time', fontsize=12, fontweight='bold', pad=15)
    ax2.legend(loc='upper left', framealpha=0.9, fontsize=9)
    ax2.grid(True, alpha=0.6, linestyle='-', linewidth=0.8, which='major', color='gray')
    ax2.grid(True, alpha=0.3, linestyle=':', linewidth=0.5, which='minor', color='gray')
    ax2.set_xscale('log')
    ax2.set_yscale('log', base=2)
    ax2.set_facecolor('white')
    
    # Add overall title
    fig.suptitle('Partial Changesets Performance (Buffered Sync)', 
                 fontsize=14, fontweight='bold', y=0.98)
    
    # Add a subtle background
    fig.patch.set_facecolor('white')
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    # Save the chart
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Partials chart saved to: {output_file}")
    
    # Also save as SVG for better quality in presentations
    svg_file = output_file.with_suffix('.svg')
    plt.savefig(svg_file, format='svg', bbox_inches='tight')
    print(f"Partials SVG chart saved to: {svg_file}")
    
    plt.close()


def main():
    # Find the Criterion target directory
    repo_root = Path(__file__).parent.parent
    target_dir = repo_root / "target" / "criterion"
    
    if not target_dir.exists():
        print(f"Error: Criterion results directory not found: {target_dir}", file=sys.stderr)
        print("Please run the benchmarks first with: cargo bench", file=sys.stderr)
        sys.exit(1)
    
    print(f"Reading benchmark results from: {target_dir}")
    results = parse_criterion_results(target_dir)
    
    if not results:
        print("Error: No benchmark results found!", file=sys.stderr)
        sys.exit(1)
    
    # Print summary
    print("\nBenchmark Results Summary:")
    print("=" * 80)
    for bench_name, batch_results in results.items():
        print(f"\n{BENCHMARK_LABELS[bench_name]}:")
        for batch_size in BATCH_SIZES:
            data = batch_results.get(batch_size)
            if data:
                print(f"  Batch {batch_size:3d} ({data['elements']:3d} elements): {data['throughput']:7.2f} Kelem/s")
    print("=" * 80)
    
    # Ensure the benchmark_results directory exists
    benchmark_results_dir = repo_root / "benchmark_results"
    benchmark_results_dir.mkdir(parents=True, exist_ok=True)

    # Generate the main chart (full changesets)
    output_file = benchmark_results_dir / "sync_fulls.png"
    generate_chart(results, output_file)
    
    # Generate the partials chart (if data exists)
    partials_output_file = benchmark_results_dir / "sync_partials.png"
    generate_partials_chart(results, partials_output_file)
    
    print(f"\n✓ Chart generation complete!")

if __name__ == "__main__":
    main()
