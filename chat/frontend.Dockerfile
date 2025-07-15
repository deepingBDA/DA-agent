FROM node:20-alpine as deps

WORKDIR /app

ARG NPM_TOKEN
RUN echo "@deepingsource:registry=https://npm.pkg.github.com/deepingsource" > .npmrc && echo "//npm.pkg.github.com/:_authToken=${NPM_TOKEN}" >> .npmrc

# 의존성 파일 복사 및 설치
COPY frontend/package*.json ./
RUN npm ci

FROM node:20-alpine as builder

WORKDIR /app

ARG VITE_API_URL
ENV VITE_API_URL=${VITE_API_URL}

COPY --from=deps /app/node_modules ./node_modules
COPY frontend/ ./

RUN npm run build

FROM nginx:alpine

WORKDIR /app

COPY --from=builder /app/build /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]