#!/usr/bin/env bash
# Analyse CAS metrics from the latest maelstrom test run.
set -euo pipefail

STORE="store/latest"
LOGS="$STORE/node-logs"

# Extract only our structured JSON log lines from all node logs.
CAS=$(rg --no-filename '"msg":"cas".*"op":"send"' "$LOGS"/*.log)

total=$(echo "$CAS" | wc -l)
failed=$(echo "$CAS" | jq --raw-output 'select(.result == "precondition failed") | .result' | wc -l)
ok=$(echo "$CAS" | jq --raw-output 'select(.result == "ok") | .result' | wc -l)

RESULTS="$STORE/results.edn"

valid=$(grep --only-matching ':valid? true\|:valid? false' "$RESULTS" | tail --lines=1)
if [[ "$valid" == ":valid? false" ]]; then
    echo "TEST FAILED - results may be misleading"
    echo
fi

echo "=== results ==="
printf "availability:   %s\n" "$(rg --only-matching 'ok-fraction [0-9.]+' "$RESULTS" | awk '{print $2}')"
printf "msgs/op (all):  %s\n" "$(rg --only-matching 'msgs-per-op [0-9.]+' "$RESULTS" | head --lines=1 | awk '{print $2}')"
printf "msgs/op (srv):  %s\n" "$(rg --only-matching 'msgs-per-op [0-9.]+' "$RESULTS" | tail --lines=1 | awk '{print $2}')"
printf "worst lag (s):  %s\n" "$(rg --only-matching ':lag [0-9.]+' "$RESULTS" | awk '{print $2}')"
printf "send ok:        %s\n" "$(rg --pcre2 --only-matching '(?<=:send \{):valid\? \w+' "$RESULTS" | awk '{print $2}')"
echo

echo "=== CAS operations (op=send) ==="
printf "total:  %d\n" "$total"
printf "ok:     %d\n" "$ok"
printf "failed: %d\n" "$failed"
if [[ $total -gt 0 ]]; then
    ratio=$(echo "scale=1; $failed * 100 / $total" | bc)
    printf "ratio:  %s%%\n" "$ratio"
fi
