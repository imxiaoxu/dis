#!/usr/bin/env bash
set -e

# === 配置信息 ===
KEY="$HOME/.ssh/iii707.pem"                    # 你的私钥路径
HOST="ec2-user@3.94.86.92"                 # 你的新 EC2 公网 IP
REMOTE_SCRIPT="~/dis/manage_workers.sh"        # 刚才在 EC2 里建的脚本路径

WORKERS_LIST=("1" "2" "3" "4" "5" "6" "7" "8" "9" "10")                # 想测试的 worker 数量
TEST_CASE="TestGol/512x512x100-1$"
RUNS_PER_CONF=3                                # 每种配置跑几次

# 项目本地路径
cd /Users/bowenxu/Downloads/gol-skeleton-master

echo "===== 自动 Worker 基准测试开始 ====="
echo "测试用例: $TEST_CASE"
echo "Worker 组合: ${WORKERS_LIST[*]}"
echo

for W in "${WORKERS_LIST[@]}"; do
  echo "========================================="
  echo " 配置远程 EC2: 启动 $W 个 worker..."
  ssh -i "$KEY" "$HOST" "$REMOTE_SCRIPT $W"

  echo "⏱  本地 go test：Workers = $W"
  for ((r=1; r<=RUNS_PER_CONF; r++)); do
    echo "--- Workers = $W, Run #$r ---"
    time go test ./tests -run "$TEST_CASE" -count=1
    echo
  done
done

echo " 所有 Worker 配置测试完成。"

