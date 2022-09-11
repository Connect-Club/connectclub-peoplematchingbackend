FROM node:latest
WORKDIR /usr/src/app/backend
RUN apt-get update && \
    apt-get install -y nano less curl unzip iputils-ping cron && \
    apt-get clean
COPY package.json .
RUN npm install
COPY . .
EXPOSE 8000
CMD ["npm", "start"]
