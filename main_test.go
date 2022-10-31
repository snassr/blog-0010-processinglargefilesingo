package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	tableTests := []struct {
		file     string
		expected result
	}{
		{
			file: "./data/itcont_sample_40.txt",
			expected: result{
				numRows:           40,
				peopleCount:       35,
				commonName:        "LAURA",
				commonNameCount:   4,
				donationMonthFreq: map[string]int{"01": 7, "02": 2, "03": 6, "04": 2, "05": 3, "06": 2, "07": 1, "08": 2, "11": 15},
			},
		},
		{
			file: "./data/itcont_sample_4000.txt",
			expected: result{
				numRows:           4000,
				peopleCount:       35,
				commonName:        "LAURA",
				commonNameCount:   400,
				donationMonthFreq: map[string]int{"01": 700, "02": 200, "03": 600, "04": 200, "05": 300, "06": 200, "07": 100, "08": 200, "11": 1500},
			},
		},
	}

	for _, tt := range tableTests {
		require.Equal(t, tt.expected, sequential(tt.file))
		require.Equal(t, tt.expected, concurrent(tt.file, 2, 10))
	}
}

func Benchmark(b *testing.B) {
	tableBenchmarks := []struct {
		name    string
		file    string
		inputs  [][]int
		benchFn func(file string, numWorkers, batchSize int) result
	}{
		{
			name:   "Sequential",
			file:   "./data/itcont.txt",
			inputs: [][]int{{0, 0}},
			benchFn: func(file string, numWorkers, batchSize int) result {
				return sequential(file)
			},
		},
		{
			name:   "Concurrent",
			file:   "./data/itcont.txt",
			inputs: [][]int{{1, 1}, {1, 1000}, {10, 1000}, {10, 10000}, {10, 100000}},
			benchFn: func(file string, numWorkers, batchSize int) result {
				return concurrent(file, numWorkers, batchSize)
			},
		},
	}

	for _, tb := range tableBenchmarks {
		for _, x := range tb.inputs {
			numWorkers := x[0]
			batchSize := x[1]

			bName := fmt.Sprintf("%s %03d workers %04d batchSize", tb.name, numWorkers, batchSize)
			b.Run(bName, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					tb.benchFn(tb.file, numWorkers, batchSize)
				}
			})
		}
	}
}

type result struct {
	numRows           int
	peopleCount       int
	commonName        string
	commonNameCount   int
	donationMonthFreq map[string]int
}

// processRow takes a pipe-separated line and returns the firstName, fullName, and month.
// this function was created to be somewhat compute intensive and not accurate.
func processRow(text string) (firstName, fullName, month string) {
	row := strings.Split(text, "|")

	// extract full name
	fullName = strings.Replace(strings.TrimSpace(row[7]), " ", "", -1)

	// extract first name
	name := strings.TrimSpace(row[7])
	if name != "" {
		startOfName := strings.Index(name, ", ") + 2
		if endOfName := strings.Index(name[startOfName:], " "); endOfName < 0 {
			firstName = name[startOfName:]
		} else {
			firstName = name[startOfName : startOfName+endOfName]
		}
		if strings.HasSuffix(firstName, ",") {
			firstName = strings.Replace(firstName, ",", "", -1)
		}
	}

	// extract month
	date := strings.TrimSpace(row[13])
	if len(date) == 8 {
		month = date[:2]
	} else {
		month = "--"
	}

	return firstName, fullName, month
}

// sequential processes a file line by line using processRow.
func sequential(file string) result {
	res := result{donationMonthFreq: map[string]int{}}

	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	// track full names
	fullNamesRegister := make(map[string]bool)

	// track first name frequency
	firstNameMap := make(map[string]int)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		row := scanner.Text()
		firstName, fullName, month := processRow(row)

		// add fullname
		fullNamesRegister[fullName] = true

		// update common firstName
		firstNameMap[firstName]++
		if firstNameMap[firstName] > res.commonNameCount {
			res.commonName = firstName
			res.commonNameCount = firstNameMap[firstName]
		}
		// add month freq
		res.donationMonthFreq[month]++
		// update numRows
		res.numRows++
		res.peopleCount = len(fullNamesRegister)
	}

	return res
}

// concurrent processes a file by splitting the file
// processing the files concurrently and returning the result.
func concurrent(file string, numWorkers, batchSize int) (res result) {
	res = result{donationMonthFreq: map[string]int{}}

	type processed struct {
		numRows    int
		fullNames  []string
		firstNames []string
		months     []string
	}

	// open file
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	// reader creates and returns a channel that recieves
	// batches of rows (of length batchSize) from the file
	reader := func(ctx context.Context, rowsBatch *[]string) <-chan []string {
		out := make(chan []string)

		scanner := bufio.NewScanner(f)

		go func() {
			defer close(out) // close channel when we are done sending all rows

			for {
				scanned := scanner.Scan()

				select {
				case <-ctx.Done():
					return
				default:
					row := scanner.Text()
					// if batch size is complete or end of file, send batch out
					if len(*rowsBatch) == batchSize || !scanned {
						out <- *rowsBatch
						*rowsBatch = []string{} // clear batch
					}
					*rowsBatch = append(*rowsBatch, row) // add row to current batch
				}

				// if nothing else to scan return
				if !scanned {
					return
				}
			}
		}()

		return out
	}

	// worker takes in a read-only channel to recieve batches of rows.
	// After it processes each row-batch it sends out the processed output
	// on its channel.
	worker := func(ctx context.Context, rowBatch <-chan []string) <-chan processed {
		out := make(chan processed)

		go func() {
			defer close(out)

			p := processed{}
			for rowBatch := range rowBatch {
				for _, row := range rowBatch {
					firstName, fullName, month := processRow(row)
					p.fullNames = append(p.fullNames, fullName)
					p.firstNames = append(p.firstNames, firstName)
					p.months = append(p.months, month)
					p.numRows++
				}
			}
			out <- p
		}()

		return out
	}

	// combiner takes in multiple read-only channels that receive processed output
	// (from workers) and sends it out on it's own channel via a multiplexer.
	combiner := func(ctx context.Context, inputs ...<-chan processed) <-chan processed {
		out := make(chan processed)

		var wg sync.WaitGroup
		multiplexer := func(p <-chan processed) {
			defer wg.Done()

			for in := range p {
				select {
				case <-ctx.Done():
				case out <- in:
				}
			}
		}

		// add length of input channels to be consumed by mutiplexer
		wg.Add(len(inputs))
		for _, in := range inputs {
			go multiplexer(in)
		}

		// close channel after all inputs channels are closed
		go func() {
			wg.Wait()
			close(out)
		}()

		return out
	}

	// create a main context, and call cancel at the end, to ensure all our
	// goroutines exit without leaving leaks.
	// Particularly, if this function becomes part of a program with
	// a longer lifetime than this function.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// STAGE 1: start reader
	rowsBatch := []string{}
	rowsCh := reader(ctx, &rowsBatch)

	// STAGE 2: create a slice of processed output channels with size of numWorkers
	// and assign each slot with the out channel from each worker.
	workersCh := make([]<-chan processed, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workersCh[i] = worker(ctx, rowsCh)
	}

	firstNameCount := map[string]int{}
	fullNameCount := map[string]bool{}

	// STAGE 3: read from the combined channel and calculate the final result.
	// this will end once all channels from workers are closed!
	for processed := range combiner(ctx, workersCh...) {
		// add number of rows processed by worker
		res.numRows += processed.numRows

		// add months processed by worker
		for _, month := range processed.months {
			res.donationMonthFreq[month]++
		}

		// use full names to count people
		for _, fullName := range processed.fullNames {
			fullNameCount[fullName] = true
		}
		res.peopleCount = len(fullNameCount)

		// update most common first name based on processed results
		for _, firstName := range processed.firstNames {
			firstNameCount[firstName]++

			if firstNameCount[firstName] > res.commonNameCount {
				res.commonName = firstName
				res.commonNameCount = firstNameCount[firstName]
			}
		}
	}

	return res
}
