package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

type Chunk struct {
	start int64
	end   int64
}

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

var measurementsFile = flag.String("measurements", "", "file with measurements")
var traceFile = flag.String("trace", "", "write trace execution to `file`")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

var MAX_CITY_NUM = 10000

func main() {
	log.Println("Starting the application...")
	flag.Parse()

	if *measurementsFile == "" {
		log.Fatal("Missing measurements filename")
	}

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

	calculate(*measurementsFile)
	//calculateWithMMap(*measurementsFile)

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

func calculate(measurementsFile string) {
	file, err := os.Open(measurementsFile)
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
	results := make(chan map[string]*TemperatureStats, numWorkers+1)
	stats := make(map[string]*TemperatureStats, MAX_CITY_NUM)

	var wg sync.WaitGroup

	start := int64(0)
	end := int64(0)
	for i := 0; i < numWorkers; i++ {
		end = start + chunkSize
		if end > fileSize {
			end = fileSize
		}

		// Read bytes until you find a newline character, indicating the start of a new line
		b := make([]byte, 1)
		for {
			//fmt.Printf("Worker %d - Reading file from %d up to %d\n", i, start, end)
			_, err := file.ReadAt(b, end-1)
			if err != nil {
				fmt.Printf("Worker %d - Error reading file from %d up to %d: %s\n", i+1, start, end, err)
				return
			}

			// fmt.Printf("Line Worker %d - read byte file: %c\n", id, b[0])
			if b[0] == '\n' || end >= fileSize {
				//fmt.Printf("Worker %d - Found newline at %d\n", i, end)
				break
			}
			end += 1
		}

		chunk := Chunk{start, end}

		log.Printf("Adding worker %d to read file from %d up to %d \n", i+1, start, end)
		wg.Add(1)
		//go processLines(i+1, chunk, file, &wg, results)
		go processLinesWithScanner(i+1, chunk, measurementsFile, &wg, results)
		start = end + 1

	}

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

func calculateWithMMap(measurementsFile string) {
	file, err := os.Open(measurementsFile)
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
	results := make(chan map[string]*TemperatureStats, numWorkers+1)
	stats := make(map[string]*TemperatureStats, MAX_CITY_NUM)

	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Fatalf("Mmap: %v", err)
	}

	defer func() {
		if err := syscall.Munmap(data); err != nil {
			log.Fatalf("Munmap: %v", err)
		}
	}()

	var wg sync.WaitGroup

	start := int64(0)
	end := int64(0)
	for i := 0; i < numWorkers; i++ {
		end = start + chunkSize
		if end > fileSize {
			end = fileSize
		}

		// Read bytes until you find a newline character, indicating the start of a new line
		for {
			// fmt.Printf("Line Worker %d - read byte file: %c\n", id, b[0])
			if data[end-1] == '\n' || end >= fileSize {
				//fmt.Printf("Worker %d - Found newline at %d\n", i, end)
				break
			}
			end += 1
		}

		chunk := Chunk{start, end}

		log.Printf("Adding worker %d to read file from %d up to %d \n", i+1, start, end)
		wg.Add(1)
		go processLinesWithMMap(i+1, chunk, data, &wg, results)
		start = end + 1

	}

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

func processLines(id int, chunk Chunk, file *os.File, wg *sync.WaitGroup, results chan<- map[string]*TemperatureStats) {
	defer wg.Done()

	stats := make(map[string]*TemperatureStats, MAX_CITY_NUM)
	b := make([]byte, chunk.end-chunk.start)
	// Process lines until the end of this chunk
	//r := bufio.NewReader(file)

	_, err := file.ReadAt(b, chunk.start)
	if err != nil {
		fmt.Printf("Worker %d - Error reading file: %s\n", id, err)
		return
	}

	offset := 0
	for offset < len(b) {

		newLinePos := bytes.IndexByte(b[offset:], '\n')
		line := string(b[offset : offset+newLinePos])
		//fmt.Printf("Worker %d - Processing line: %s\n", id, line)
		// Process the line
		parts := strings.Split(line, ";")
		city := parts[0]
		temperature, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			fmt.Printf("Worker %d - Error parsing flat: %s\n", id, err)
			return
		}

		recording, exists := stats[city]
		if !exists {
			stats[city] = &TemperatureStats{temperature, temperature, temperature, 1, 0.0}
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

		offset += (newLinePos + 1)

	}

	results <- stats
}

func processLinesWithScanner(id int, chunk Chunk, filePath string, wg *sync.WaitGroup, results chan<- map[string]*TemperatureStats) {
	defer wg.Done()

	stats := make(map[string]*TemperatureStats, MAX_CITY_NUM)
	// Process lines until the end of this chunk
	//r := bufio.NewReader(file)

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	currentPosition := chunk.start

	// Seek to the start position
	_, err2 := file.Seek(chunk.start, 0)
	if err2 != nil {
		fmt.Printf("Worker %d - Error seeking file: %s\n", id, err)
		return
	}

	for scanner.Scan() {
		currentPosition += int64(len(scanner.Bytes())) + 1 // +1 for newline character
		if currentPosition > chunk.end {
			break
		}

		line := scanner.Text()
		//fmt.Printf("Worker %d - Processing line: %s\n", id, line)
		// Process the line
		parts := strings.Split(line, ";")
		city := parts[0]
		temperature, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			fmt.Printf("Worker %d - Error parsing flat: %s\n", id, err)
			return
		}

		recording, exists := stats[city]
		if !exists {
			stats[city] = &TemperatureStats{temperature, temperature, temperature, 1, 0.0}
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

func processLinesWithMMap(id int, chunk Chunk, data []byte, wg *sync.WaitGroup, results chan<- map[string]*TemperatureStats) {
	defer wg.Done()

	stats := make(map[string]*TemperatureStats, MAX_CITY_NUM)
	// Process lines until the end of this chunk
	//r := bufio.NewReader(file)

	b := data[chunk.start:chunk.end]

	offset := 0
	for offset < len(b) {

		newLinePos := bytes.IndexByte(b[offset:], '\n')
		line := string(b[offset : offset+newLinePos])
		//fmt.Printf("Worker %d - Processing line: %s\n", id, line)
		// Process the line
		parts := strings.Split(line, ";")
		city := parts[0]
		temperature, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			fmt.Printf("Worker %d - Error parsing flat: %s\n", id, err)
			return
		}

		recording, exists := stats[city]
		if !exists {
			stats[city] = &TemperatureStats{temperature, temperature, temperature, 1, 0.0}
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

		offset += (newLinePos + 1)

	}

	results <- stats
}
