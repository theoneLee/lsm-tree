

build:
	go build

test:
	go test ./... -coverprofile=coverage.out

test_cover:
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out


run:
	go run example.go
