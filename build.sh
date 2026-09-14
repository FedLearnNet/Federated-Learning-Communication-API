env CGO_ENABLED=1 go build -o dist/fc_ctrl_linux64 cmd/controller/main.go

env GOOS=darwin GOARCH=amd64 go build -o dist/fc_ctrl_macos64 cmd/controller/main.go

env GOOS=windows GOARCH=amd64 go build -o dist/fc_ctrl_windows64.exe cmd/controller/main.go

tar -czvf fc_ctrl.tar.gz dist
