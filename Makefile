PY ?= python
PORT ?= 8080
TOKEN ?= change-me
DATA ?= ./data

.PHONY: help install test run worker seed bench docker-up docker-down clean

help:
	@echo "make install   安装（可编辑模式 + 开发依赖）"
	@echo "make test      运行全部测试"
	@echo "make run       启动 API（$(PORT)）"
	@echo "make worker    启动后台维护进程"
	@echo "make seed      装载演示数据"
	@echo "make bench     压测（64KiB / 8 并发 / 6s）"
	@echo "make docker-up 容器启动 api + worker"
	@echo "make clean     清理本地数据与缓存"

install:
	$(PY) -m pip install -e ".[dev]"

test:
	$(PY) -m pytest -p no:warnings

run:
	STRATUM_ADMIN_TOKEN=$(TOKEN) STRATUM_DATA_DIR=$(DATA) \
		$(PY) -m uvicorn stratum.main:create_app --factory --host 127.0.0.1 --port $(PORT)

worker:
	STRATUM_ADMIN_TOKEN=$(TOKEN) STRATUM_DATA_DIR=$(DATA) \
		$(PY) -m stratum.worker --interval 5

seed:
	$(PY) scripts/seed_demo.py --base-url http://127.0.0.1:$(PORT) --token $(TOKEN)

bench:
	$(PY) scripts/bench.py --base-url http://127.0.0.1:$(PORT) --token $(TOKEN) \
		--bucket bench --size 65536 --concurrency 8 --seconds 6

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down

clean:
	find . -name __pycache__ -type d -prune -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache
