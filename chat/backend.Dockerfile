FROM python:3.12-slim

# Python 버퍼링 비활성화 (로그 즉시 출력)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Node.js 및 SSH 관련 시스템 의존성 설치
RUN apt-get update && apt-get install -y \
    curl \
    openssh-client \
    libffi-dev \
    libssl-dev \
    build-essential \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# 의존성 파일 복사 및 설치
COPY requirements-backend.txt .
RUN pip install --no-cache-dir -r requirements-backend.txt

# 현재 디렉토리의 모든 파일을 컨테이너로 복사
COPY . .

# 포트 설정
EXPOSE 8000

# 실행 명령어
CMD ["uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000"] 