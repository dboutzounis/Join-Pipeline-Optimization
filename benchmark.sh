#!/bin/bash

executables=(
  "./build/fast"
)

plan_file="plans.json"
runs=3
output_dir="benchmark_outputs"

mkdir -p "$output_dir"

if command -v gtime >/dev/null 2>&1; then
  TIME_CMD="gtime -v"
elif /usr/bin/time -v true >/dev/null 2>&1; then
  TIME_CMD="/usr/bin/time -v"
else
  echo "ERROR: GNU time not found. Install with: brew install gnu-time"
  exit 1
fi

echo "Using time command: $TIME_CMD"
echo "Running benchmarks..."
echo

for exe in "${executables[@]}"; do
  name=$(basename "$exe")
  exe_dir="$output_dir/$name"
  mkdir -p "$exe_dir"

  total_time=0
  total_cpu=0
  max_rss=0

  echo "==> ${exe}"

  for ((i=1; i<=runs; i++)); do
    echo "Run $i..."

    out_file="$exe_dir/${name}_run${i}.out"
    time_file="$exe_dir/${name}_run${i}.time"

    $TIME_CMD $exe "$plan_file" \
      > "$out_file" \
      2> "$time_file"

    user_time=$(grep "User time (seconds)" "$time_file" | sed 's/.*: //')
    sys_time=$(grep "System time (seconds)" "$time_file" | sed 's/.*: //')
    cpu_pct=$(grep "Percent of CPU this job got" "$time_file" | sed 's/.*: //' | tr -d '%')
    rss=$(grep "Maximum resident set size" "$time_file" | sed 's/.*: //')

    wall_time=$(echo "$user_time + $sys_time" | bc)

    total_time=$(echo "$total_time + $wall_time" | bc)
    total_cpu=$(echo "$total_cpu + $cpu_pct" | bc)

    if (( rss > max_rss )); then
      max_rss=$rss
    fi

    echo "  Time (user+sys): ${wall_time}s"
    echo "  CPU usage: ${cpu_pct}%"
    echo "  Peak RSS: ${rss} KB"
  done

  avg_time=$(echo "scale=4; $total_time / $runs" | bc)
  avg_cpu=$(echo "scale=2; $total_cpu / $runs" | bc)

  summary_file="$exe_dir/summary.txt"
  {
    echo "Executable: $exe"
    echo "Runs: $runs"
    echo "Average execution time (s): $avg_time"
    echo "Average CPU usage (%): $avg_cpu"
    echo "Peak memory usage (KB): $max_rss"
    echo "Platform: $(uname -s)"
    echo "CPU: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || lscpu | grep 'Model name')"
  } > "$summary_file"

  echo
  echo "Summary for ${exe}:"
  cat "$summary_file"
  echo
done