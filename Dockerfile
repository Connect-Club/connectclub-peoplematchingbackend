FROM node:17.6.0-buster
WORKDIR /usr/src/app/backend
COPY package.json .
RUN npm install
COPY . .
EXPOSE 8000
CMD ["npm", "run", "prod"]
