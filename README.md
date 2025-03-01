<div align="center">
<h1>Parquet-Go Examples</h1>
<p>
explore parquet-go with examples covering installation, data operations, and performance benchmarks
</p>
</div>

## About

While Parquet defines the storage format, you need a tool to interact with it. Parquet-Go is a library for the Go programming language that enables reading and writing Parquet files. It is hosted at [parquet-go/parquet-go](https://github.com/parquet-go/parquet-go), and I had the opportunity to contribute to its development. A notable feature of Parquet-Go is its independence from external dependencies—it operates entirely within Go, simplifying integration.

## Usage

#### Installation and Setup

Ensure Go is installed (version 1.21 or later is recommended), then install Parquet-Go:

```bash
go get github.com/parquet-go/parquet-go
```

#### Data Structure

First, define your data structure with Parquet tags:

```go
// Person struct with minimal Parquet tags
type Person struct {
	Name        string  `parquet:"name"`
	Age         int32   `parquet:"age"`
	Email       string  `parquet:"email,optional"`
	Score1      int32   `parquet:"score1"`
	Score2      int32   `parquet:"score2"`
	Score3      int32   `parquet:"score3"`
	Score4      int32   `parquet:"score4"`
	Score5      int32   `parquet:"score5"`
	Balance     float64 `parquet:"balance"`
	Expenditure float64 `parquet:"expenditure"`
}
```

#### Combined Read/Write Operations with Benchmarking

The `main.go` file contains examples of reading and writing Parquet files with Parquet-Go, along with a comparison to CSV and JSON formats.

This combined approach shows how Parquet-Go delivers superior performance across all dimensions that matter for data processing: speed, storage efficiency, and analytical capabilities.
