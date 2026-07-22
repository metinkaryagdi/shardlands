#!/usr/bin/env bash
# Yerel kümeyi tamamen siler (PVC'ler dahil).
set -euo pipefail
kind delete cluster --name shardlands
