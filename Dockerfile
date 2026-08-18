FROM golang:1.26.4-alpine

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .

RUN go build -o node .

EXPOSE 9000

CMD ["./node"]