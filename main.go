package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"
)

type Temps struct {
	min float64
	max float64
	avg float64
}

type Measurements struct {
	min, max, count int
	avg             float64
}

func NewMeasurement() Measurements {
	return Measurements{
		min:   1000,
		max:   -1000,
		count: 0,
		avg:   0,
	}
}

func main() {
	// Create a CPU profile file
	cpuProf, err := os.Create("cpu.prof")
	if err != nil {
		panic(err)
	}
	defer cpuProf.Close()

	// Start CPU profiling
	if err := pprof.StartCPUProfile(cpuProf); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	// Create a memory profile file
	memProf, err := os.Create("mem.prof")
	if err != nil {
		panic(err)
	}
	defer memProf.Close()

	// Write memory profile to file
	if err := pprof.WriteHeapProfile(memProf); err != nil {
		panic(err)
	}

	start := time.Now()
	chunkedReadWithWorkerPool("./measurements.txt")
	end := time.Now()
	fmt.Printf("finished in %v seconds", end.Sub(start).Seconds())
}

var workers int = runtime.NumCPU()

const chunkSize int = 4 * 1024 * 1024

func chunkedReadWithWorkerPool(filename string) {
	chunks := make(chan []byte, 100)
	res := make(chan map[string]Measurements)
	done := make(chan interface{})
	wg := &sync.WaitGroup{}
	aggregate := make(map[string]Measurements)

	for i := 0; i < workers; i++ {
		go func() {
			wg.Add(1)
			worker(chunks, res, wg)
		}()
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	readChunks(filename, chunks)

label:
	for {
		select {
		case <-done:
			break label
		case x := <-res:
			for c, m := range x {
				var agg Measurements
				v, ok := aggregate[c]
				if ok {
					agg = v
				} else {
					agg = NewMeasurement()
				}
				totalCount := agg.count + m.count
				agg.min = min(agg.min, m.min)
				agg.max = max(agg.max, m.max)
				agg.count += m.count
				agg.avg = ((agg.avg * float64(agg.count)) + (m.avg * float64(m.count))) / float64(totalCount)
				aggregate[c] = agg
			}
		}
	}
	close(res)

	keys := make([]string, len(aggregate))
	for k := range aggregate {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("{")
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%.1f/%.1f/%.1f,", k, float64(aggregate[k].min)/float64(10), aggregate[k].avg, float64(aggregate[k].max)/float64(10))
	}
	b.WriteString("}")

	fmt.Println(b.String())
}

func worker(chunks <-chan []byte, res chan<- map[string]Measurements, wg *sync.WaitGroup) {
	log.Println("starting worker")
	defer wg.Done()
	length := 0
	reads := 0
	lines := 0
	newLine := byte('\n')
	word := []byte{}
	measurements := make(map[string]Measurements, 250)
	for in := range chunks {
		lineBoundary := 0
		length += len(in)
		reads++
		for i := 0; i < len(in); i++ {
			if in[i] == newLine || i == len(in)-1 { //handle last line
				lines++
				word = in[lineBoundary:i]
				lineBoundary = i + 1
				city, temp := parseLine(word)
				m, ok := measurements[city]
				if !ok {
					m = NewMeasurement()
				}
				m.count++
				m.min = min(m.min, temp)
				m.max = max(m.max, temp)
				m.avg = m.avg + (float64(temp)-m.avg)/float64(m.count)
				measurements[city] = m
				word = word[:0]
			}
		}
	}
	log.Printf("read %d bytes, %d lines, in %d reads\n", length, lines, reads)
	res <- measurements
}

const semi byte = byte(';')
const period = byte('.')
const neg = byte('-')

func parseLine(line []byte) (string, int) {
	var city string

	splitIndex := 0
	for i := 0; i < len(line); i++ {
		if line[i] == semi {
			city = string(line[0:i])
			splitIndex = i
		}
	}

	num := 0
	mult := 1
	for i := splitIndex + 1; i < len(line); i++ {
		if line[i] == period {
			continue
		}
		if line[i] == neg {
			mult = -1
			continue
		}
		b := line[i]
		num = num*10 + int(b-'0')
	}
	return city, num * mult
}

func readChunks(filename string, chunks chan []byte) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	newLine := byte('\n')
	r := bufio.NewReaderSize(file, chunkSize+64)
	for {
		buf := make([]byte, chunkSize)
		n, err := r.Read(buf)
		if n == 0 {
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf(err.Error())
			}
		}
		b, err := r.ReadBytes(newLine)
		if err == nil || err == io.EOF {
			buf = append(buf, b...)
		} else {
			fmt.Printf(err.Error())
		}
		chunks <- buf
	}
	log.Println("finished reading file. Closing channel")
	close(chunks)
}
