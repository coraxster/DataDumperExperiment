##Data dumper go exp

also check branch "simple"

###build
```bash
GIT_COMMIT=$(git rev-list -1 HEAD) && go build -ldflags "-X main.version=$GIT_COMMIT" -o DDumper main.go
```

###build for Windows
```bash
GIT_COMMIT=$(git rev-list -1 HEAD) && GOOS=windows;GOARCH=386 go build -o DDumper.exe main.go
```

###lock file using lockfile
```bash
lockfile /path/to/your.file
```

###lock file for Windows
```bash
powershell -ExecutionPolicy ByPass -File C:\tmp\lockfile.PS1 -file C:\tmp\data\1in/01.jpg
```