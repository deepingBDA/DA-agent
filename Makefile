.PHONY: server help pull build up down logs status

# Default target
help:
	@echo "Available commands:"
	@echo "  make server  - Pull latest changes and restart docker services"
	@echo "  make pull    - Pull latest changes from git"
	@echo "  make build   - Build docker images"
	@echo "  make up      - Start docker services"
	@echo "  make down    - Stop docker services"
	@echo "  make logs    - Show docker logs"
	@echo "  make status  - Show docker container status"

# Main server command - pull and restart
server: pull down up
	@echo "🎉 Server updated and restarted successfully!"

# Git pull
pull:
	@echo "📥 Pulling latest changes from git..."
	git pull origin main

# Docker commands
down:
	@echo "🔽 Stopping docker services..."
	docker compose down

up:
	@echo "🔼 Starting docker services..."
	docker compose up -d --build

build:
	@echo "🔨 Building docker images..."
	docker compose build

logs:
	@echo "📋 Showing docker logs..."
	docker compose logs -f

status:
	@echo "📊 Docker container status:"
	docker compose ps