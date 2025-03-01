package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"
)

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

// generateDataset creates sample data
func generateDataset(n int) []Person {
	dataset := make([]Person, n)
	for i := 0; i < n; i++ {
		dataset[i] = Person{
			Name:        fmt.Sprintf("Person_%d", i),
			Age:         int32(i % 100),
			Email:       fmt.Sprintf("person%d@example.com", i),
			Score1:      int32(i % 1000),
			Score2:      int32(i % 1000),
			Score3:      int32(i % 1000),
			Score4:      int32(i % 1000),
			Score5:      int32(i % 1000),
			Balance:     float64(i) * 1.5,
			Expenditure: float64(i) * 2.5,
		}
	}
	return dataset
}

// Parquet operations
func parquetOperations(dataset []Person) (writeTime, readTime time.Duration, fileSize int64) {
	// Write operation
	startWrite := time.Now()
	file, err := os.Create("people.parquet")
	if err != nil {
		log.Fatalf("Failed to create Parquet file: %v", err)
	}
	defer file.Close()

	wr := parquet.NewWriter(file, parquet.SchemaOf(&Person{}))
	for _, p := range dataset {
		if err := wr.Write(p); err != nil {
			log.Fatalf("Failed to write to Parquet: %v", err)
		}
	}
	if err := wr.Close(); err != nil {
		log.Fatalf("Failed to close Parquet writer: %v", err)
	}
	writeTime = time.Since(startWrite)

	// Get file size
	fileInfo, err := os.Stat("people.parquet")
	if err != nil {
		log.Fatalf("Failed to get Parquet file info: %v", err)
	}
	fileSize = fileInfo.Size()

	// Read operation
	startRead := time.Now()
	file, err = os.Open("people.parquet")
	if err != nil {
		log.Fatalf("Failed to open Parquet file: %v", err)
	}
	defer file.Close()

	rd := parquet.NewReader(file, parquet.SchemaOf(&Person{}))
	var people []Person
	for {
		p := Person{}
		err := rd.Read(&p)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Failed to read Parquet row: %v", err)
		}
		people = append(people, p)
	}

	// Using the people slice (length check) to avoid the unused result warning
	_ = len(people)
	if err := rd.Close(); err != nil {
		log.Fatalf("Failed to close Parquet reader: %v", err)
	}
	readTime = time.Since(startRead)

	return writeTime, readTime, fileSize
}

// CSV operations
func csvOperations(dataset []Person) (writeTime, readTime time.Duration, fileSize int64) {
	// Write operation
	startWrite := time.Now()
	file, err := os.Create("people.csv")
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{"Name", "Age", "Email", "Score1", "Score2", "Score3", "Score4", "Score5", "Balance", "Expenditure"}
	if err := writer.Write(headers); err != nil {
		log.Fatalf("Failed to write CSV headers: %v", err)
	}

	for _, p := range dataset {
		record := []string{
			p.Name,
			strconv.Itoa(int(p.Age)),
			p.Email,
			strconv.Itoa(int(p.Score1)),
			strconv.Itoa(int(p.Score2)),
			strconv.Itoa(int(p.Score3)),
			strconv.Itoa(int(p.Score4)),
			strconv.Itoa(int(p.Score5)),
			strconv.FormatFloat(p.Balance, 'f', -1, 64),
			strconv.FormatFloat(p.Expenditure, 'f', -1, 64),
		}
		if err := writer.Write(record); err != nil {
			log.Fatalf("Failed to write CSV record: %v", err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Fatalf("CSV writer error: %v", err)
	}
	writeTime = time.Since(startWrite)

	// Get file size
	fileInfo, err := os.Stat("people.csv")
	if err != nil {
		log.Fatalf("Failed to get CSV file info: %v", err)
	}
	fileSize = fileInfo.Size()

	// Read operation
	startRead := time.Now()
	file, err = os.Open("people.csv")
	if err != nil {
		log.Fatalf("Failed to open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read() // Skip header
	if err != nil {
		log.Fatalf("Failed to read CSV header: %v", err)
	}

	var people []Person
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Failed to read CSV row: %v", err)
		}
		age, err := strconv.Atoi(record[1])
		if err != nil {
			log.Fatalf("Failed to parse age: %v", err)
		}
		score1, err := strconv.Atoi(record[3])
		if err != nil {
			log.Fatalf("Failed to parse score1: %v", err)
		}
		score2, err := strconv.Atoi(record[4])
		if err != nil {
			log.Fatalf("Failed to parse score2: %v", err)
		}
		score3, err := strconv.Atoi(record[5])
		if err != nil {
			log.Fatalf("Failed to parse score3: %v", err)
		}
		score4, err := strconv.Atoi(record[6])
		if err != nil {
			log.Fatalf("Failed to parse score4: %v", err)
		}
		score5, err := strconv.Atoi(record[7])
		if err != nil {
			log.Fatalf("Failed to parse score5: %v", err)
		}
		balance, err := strconv.ParseFloat(record[8], 64)
		if err != nil {
			log.Fatalf("Failed to parse balance: %v", err)
		}
		expenditure, err := strconv.ParseFloat(record[9], 64)
		if err != nil {
			log.Fatalf("Failed to parse expenditure: %v", err)
		}

		people = append(people, Person{
			Name:        record[0],
			Age:         int32(age),
			Email:       record[2],
			Score1:      int32(score1),
			Score2:      int32(score2),
			Score3:      int32(score3),
			Score4:      int32(score4),
			Score5:      int32(score5),
			Balance:     balance,
			Expenditure: expenditure,
		})
	}

	// Using people slice (length check) to avoid the unused result warning
	_ = len(people)
	readTime = time.Since(startRead)

	return writeTime, readTime, fileSize
}

// JSON operations
func jsonOperations(dataset []Person) (writeTime, readTime time.Duration, fileSize int64) {
	// Write operation
	startWrite := time.Now()
	file, err := os.Create("people.json")
	if err != nil {
		log.Fatalf("Failed to create JSON file: %v", err)
	}
	defer file.Close()

	data, err := json.Marshal(dataset)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}
	if _, err := file.Write(data); err != nil {
		log.Fatalf("Failed to write JSON: %v", err)
	}
	writeTime = time.Since(startWrite)

	// Get file size
	fileInfo, err := os.Stat("people.json")
	if err != nil {
		log.Fatalf("Failed to get JSON file info: %v", err)
	}
	fileSize = fileInfo.Size()

	// Read operation
	startRead := time.Now()
	file, err = os.Open("people.json")
	if err != nil {
		log.Fatalf("Failed to open JSON file: %v", err)
	}
	defer file.Close()

	var people []Person
	data, err = io.ReadAll(file)
	if err != nil {
		log.Fatalf("Failed to read JSON file: %v", err)
	}
	if err := json.Unmarshal(data, &people); err != nil {
		log.Fatalf("Failed to unmarshal JSON: %v", err)
	}
	readTime = time.Since(startRead)

	return writeTime, readTime, fileSize
}

func main() {
	const numRecords = 1_000_000_000

	// Generate dataset
	fmt.Printf("Generating %d records...\n", numRecords)
	dataset := generateDataset(numRecords)

	// Run benchmarks
	fmt.Printf("Running benchmarks...\n")
	parquetWrite, parquetRead, parquetSize := parquetOperations(dataset)
	csvWrite, csvRead, csvSize := csvOperations(dataset)
	jsonWrite, jsonRead, jsonSize := jsonOperations(dataset)

	// Display results
	fmt.Printf("%-12s | %-15s | %-15s | %-15s\n", "Operation", "Parquet", "CSV", "JSON")
	fmt.Println("-------------+-----------------+-----------------+-----------------")
	fmt.Printf("%-12s | %-15v | %-15v | %-15v\n", "Write Time", parquetWrite, csvWrite, jsonWrite)
	fmt.Printf("%-12s | %-15v | %-15v | %-15v\n", "Read Time", parquetRead, csvRead, jsonRead)
	fmt.Printf("%-12s | %-15d | %-15d | %-15d\n", "File Size", parquetSize, csvSize, jsonSize)
}
