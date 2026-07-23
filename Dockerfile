FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

RUN npm install pm2 -g
RUN apk add --no-cache tzdata
ENV TZ=Asia/Bangkok

EXPOSE 3000

CMD ["node", "server.js"]
