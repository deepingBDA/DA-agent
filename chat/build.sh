docker rmi langgraph-mcp-agent-frontend
docker rmi langgraph-mcp-agent-backend
docker rm -f mcp-frontend
docker rm -f mcp-backend
docker rm -f mcp-nginx
docker compose up -d --build