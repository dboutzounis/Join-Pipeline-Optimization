#!/bin/bash

executables=(
  "./build/fast"
)
plan_file="plans.json"
runs=5
output_dir="benchmark_outputs"

mkdir -p "$output_dir"

echo "Running benchmarks..."
echo

for exe in "${executables[@]}"; do
  name=$(basename "$exe")
  exe_dir="$output_dir/$name"
  mkdir -p "$exe_dir"
  total_time=0

  echo "==> ${exe}"

  for ((i=1; i<=runs; i++)); do
    echo "Run $i..."
    start=$(date +%s.%N)

    out_file="$exe_dir/${name}_run${i}.txt"
    $exe "$plan_file" > "$out_file" 2>&1

    end=$(date +%s.%N)
    runtime=$(echo "$end - $start" | bc)
    echo "  Time: ${runtime}s"
    total_time=$(echo "$total_time + $runtime" | bc)
  done

  avg_time=$(echo "scale=4; $total_time / $runs" | bc)
  echo "Average time for ${exe}: ${avg_time}s"
  echo
done