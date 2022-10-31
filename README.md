# Processing Large Files in Go (Golang)
Comparing sequential and concurrent processing of files in Go.

## Setup
Before running this program, ensure you unzip the itcont.txt.zip file to the data folder.
```bash
# install p7zip as needed
brew install p7zip
# unzip folder using p7zip
7z e data/itcont.txt.7z -odata/
```

## Test
```bash
go test -timeout 120s -run ^Test$ github.com/snassr/blog-0010-processinglargefilesingo
```

## Benchmark
```bash
go test -benchmem -run=^$ -bench ^Benchmark$ github.com/snassr/blog-0010-processinglargefilesingo
```
