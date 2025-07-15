FROM python:3.12-slim

WORKDIR /app

# Install Node.js
RUN apt-get update && apt-get install -y \
    curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*


RUN pip install --upgrade pip
RUN pip install python-dotenv langchain-openai pytz uv fastmcp clickhouse_connect tiktoken

COPY . .
RUN cd mcp-use && pip install -e .
