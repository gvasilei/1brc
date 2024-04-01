package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
)

type TemperatureStats struct {
	min   float64
	max   float64
	sum   float64
	count int
	avg   float64
}

func (stats TemperatureStats) String() string {
	return fmt.Sprintf("Min: %.2f, Max: %.2f, Avg: %.2f", stats.min, stats.max, stats.avg)
}

func processLines(id int, start, end int64, filePath string, wg *sync.WaitGroup, results chan<- map[string]TemperatureStats) {
	defer wg.Done()

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	stats := make(map[string]TemperatureStats)

	// Seek to the start position
	_, err2 := file.Seek(start, 0)
	if err2 != nil {
		fmt.Printf("Worker %d - Error seeking file: %s\n", id, err)
		return
	}

	isFirstChunk := start == 0
	/*
		We can't find the perfect chunk size because we don't know the exact position of the line breaks.
		For this reason we'll have to adjust the start and end accordingly.
	*/

	// For chunks that are not the first, skip until the end of the first line break
	if !isFirstChunk {
		// Read bytes until you find a newline character, indicating the start of a new line
		b := make([]byte, 1)
		for {
			_, err := file.Read(b)
			if err != nil {
				fmt.Printf("Worker %d - Error reading file: %s\n", id, err)
				return
			}

			// fmt.Printf("Line Worker %d - read byte file: %c\n", id, b[0])
			// Increment start position to not re-process the same byte
			start += 1
			if b[0] == '\n' || start >= end {
				break
			}
		}
	}

	scanner := bufio.NewScanner(file)
	currentPosition := start

	// Process lines until the end of this chunk
	for scanner.Scan() {
		currentPosition += int64(len(scanner.Bytes())) + 1 // +1 for newline character
		if currentPosition > end {
			break
		}
		// Process the line
		line := scanner.Text()
		parts := strings.Split(line, ";")
		city := parts[0]
		temperature, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			fmt.Printf("Worker %d - Error parsing flat: %s\n", id, err)
			return
		}

		recording, exists := stats[city]
		if !exists {
			stats[city] = TemperatureStats{math.Inf(1), math.Inf(-1), temperature, 1, 0.0}
		} else {
			if temperature < recording.min {
				recording.min = temperature
			} else if temperature > recording.max {
				recording.max = temperature
			}
			recording.sum += temperature
			recording.count++

			stats[city] = recording
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Worker %d - Error reading chunk: %s\n", id, err)
	}

	results <- stats
}

var traceFile = flag.String("trace", "", "write trace execution to `file`")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	log.Println("Starting the application...")
	flag.Parse()

	if *traceFile != "" {
		f, err := os.Create("./profiles/" + *traceFile)
		if err != nil {
			log.Fatal("Failed to create trace profile: ", err)
		}
		defer f.Close()
		err = trace.Start(f)
		if err != nil {
			log.Fatal("Failed to start trace: ", err)
		}
		defer trace.Stop()
	}

	if *cpuprofile != "" {
		f, err := os.Create("./profiles/" + *cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	calculate()

	if *memprofile != "" {
		f, err := os.Create("./profiles/" + *memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

func calculate() {
	filePath := "../../../../measurements.txt"
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file stats:", err)
		return
	}

	fileSize := fileInfo.Size()
	numWorkers := runtime.NumCPU()

	chunkSize := fileSize / int64(numWorkers)
	results := make(chan map[string]TemperatureStats, numWorkers+1)
	stats := make(map[string]TemperatureStats)

	var wg sync.WaitGroup
	workerId := 0
	for start := int64(0); start < fileSize; start += chunkSize {
		end := start + chunkSize
		if end > fileSize {
			end = fileSize
		}

		log.Printf("Adding worker %d to read file from %d up to %d \n", workerId, start, end)

		wg.Add(1)
		go processLines(workerId, start, end, filePath, &wg, results)
		workerId++
	}

	// No more tasks to be added, close the channel.
	wg.Wait()
	close(results)

	log.Println("Merging results...")
	for resultsMap := range results {
		for city, recording := range resultsMap {
			existingRecording, exists := stats[city]
			if !exists {
				stats[city] = recording
			} else {
				if recording.min < existingRecording.min {
					existingRecording.min = recording.min
				}
				if recording.max > existingRecording.max {
					existingRecording.max = recording.max
				}
				existingRecording.sum += recording.sum
				existingRecording.count += recording.count
				stats[city] = existingRecording
			}
		}
	}

	log.Println("Calculating averages...")
	for city, recording := range stats {
		recording.avg = recording.sum / float64(recording.count)
		stats[city] = recording
	}
	log.Println("Outputting stats...")

	fmt.Print(stats)
}
