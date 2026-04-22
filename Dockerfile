# ================================================================
# Dockerfile — stock_analyize 容器化配置
# 使用：docker build -t stock-analyize .
#       docker run --rm -v $(pwd)/output:/app/output -v $(pwd)/config:/app/config stock-analyize
# ================================================================
FROM python:3.11-slim

# 维护者信息
LABEL maintainer="tenseven"
LABEL description="股票量化投研分析平台"
LABEL version="1.0"

# 时区设置（中国标准时间）
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 系统依赖（matplotlib 中文字体 + 网络工具）
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    fonts-wqy-zenhei \
    fonts-wqy-microhei \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 工作目录
WORKDIR /app

# 优先复制依赖文件（充分利用 Docker 层缓存）
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 复制项目代码
COPY . .

# 创建输出目录
RUN mkdir -p logs output/data output/reports output/backtest output/plots

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import src; print('OK')" || exit 1

# 默认命令（可在 docker run 时覆盖）
CMD ["python", "main.py", "--market", "all"]
