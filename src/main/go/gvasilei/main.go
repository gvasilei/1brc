package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
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

	calculateWithMMap(*measurementsFile)

	if *memprofile != "" {
		f, err := os.Create("./profiles/" + *memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
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
	results := make(chan map[string]*TemperatureStats, numWorkers)
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

func processLinesWithMMap(id int, chunk Chunk, data []byte, wg *sync.WaitGroup, results chan<- map[string]*TemperatureStats) {
	defer wg.Done()

	stats := make(map[string]*TemperatureStats, MAX_CITY_NUM)
	// Process lines until the end of this chunk
	//r := bufio.NewReader(file)

	b := data[chunk.start:chunk.end]

	offset := 0
	for offset < len(b) {

		newLinePos := bytes.IndexByte(b[offset:], '\n')
		line := b[offset : offset+newLinePos]
		//fmt.Printf("Worker %d - Processing line: %s\n", id, line)
		// Process the line
		cityB, temperatureB, _ := bytes.Cut(line, []byte(";"))
		temperature := parseFloat(temperatureB)

		recording, exists := stats[string(cityB)]
		if !exists {
			stats[string(cityB)] = &TemperatureStats{temperature, temperature, temperature, 1, 0.0}
		} else {
			if temperature < recording.min {
				recording.min = temperature
			} else if temperature > recording.max {
				recording.max = temperature
			}
			recording.sum += temperature
			recording.count++
		}

		offset += (newLinePos + 1)

	}

	results <- stats
}

func parseFloat(b []byte) float64 {
	i := 0
	isNegative := false
	if b[0] == '-' {
		isNegative = true
		i++
	}

	temp := float64(b[i] - '0')
	i++

	if b[i] != '.' {
		temp = temp*10 + float64(b[i]-'0')
		i++
	}

	i++
	temp += float64(b[i]-'0') / 10 // parse decimal digit
	if isNegative {
		temp = -temp
	}

	return temp
}
