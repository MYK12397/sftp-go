# SFTP Pipeline

A high-performance Go library for parallel SFTP file transfers with concurrent reading and processing capabilities.

## Features

- **Parallel SFTP Reads**: Configurable number of concurrent SFTP connections for reading files
- **Concurrent Processing**: Process downloaded files with multiple worker goroutines
- **Buffered Pipeline**: Configurable buffer size between read and processing stages
- **Error Handling**: Tracks failed transfers separately from successful ones
- **Performance Metrics**: Reports transfer time and success/failure statistics

## Installation

```bash
go get github.com/MYK12397/sftp-pipeline
```

## Usage

### Basic Examples

```go
 // Write to local disk
 TransferFiles(sftpClient, jobs, func(r FileResult) error{
     return os.WriteFile("/data/"+r.ID, r.Data, 0644)
 })

//upload to cloud storage
TransferFiles(sftpClient, jobs, func(r FileResult) error{
     return cloudClient.Upload(r.ID, r.Data)
 })

TransferFiles(sftpClient, jobs, func(r FileResult) error{
     text := extractText(r.Data)
     return cloudClient.Upload(r.ID, r.Data)
 })
```
## Configuration

The `PipelineCfg` struct controls the pipeline behavior:

- **SFTPRreaders**: Number of goroutines reading from SFTP (default: 80)
- **Workers**: Number of goroutines processing files (default: 10)
- **BufferSize**: Channel buffer size (default: 10)

