##Data dumper go exp

###build
```bash
go build -o DDumper main.go
```

###build for Windows
```bash
GOOS=windows;GOARCH=386 go build -o DDumper.exe main.go
```

###lock file using lockfile
```bash
lockfile /path/to/your.file
```

###lock file for Windows
```bash
powershell -ExecutionPolicy ByPass -File C:\tmp\lockfile.PS1 -file C:\tmp\data\1in/01.jpg
```