FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app/seam ./cmd/seam
RUN CGO_ENABLED=0 go build -o /app/seam-capture ./cmd/seam-capture
RUN CGO_ENABLED=0 go build -o /app/seam-lab ./cmd/seam-lab

FROM gcr.io/distroless/static-debian12
COPY --from=builder /app/seam /app/seam
COPY --from=builder /app/seam-capture /app/seam-capture
COPY --from=builder /app/seam-lab /app/seam-lab
USER nonroot:nonroot
