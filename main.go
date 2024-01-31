package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type Temps struct {
	min float64
	max float64
	avg float64
}

func main() {
	// Create a CPU profile file
	// cpuProf, err := os.Create("cpu.prof")
	// if err != nil {
	// 	panic(err)
	// }
	// defer cpuProf.Close()

	// Start CPU profiling
	// if err := pprof.StartCPUProfile(cpuProf); err != nil {
	// 	panic(err)
	// }
	// defer pprof.StopCPUProfile()

	// Create a memory profile file
	// memProf, err := os.Create("mem.prof")
	// if err != nil {
	// 	panic(err)
	// }
	// defer memProf.Close()

	// Write memory profile to file
	// if err := pprof.WriteHeapProfile(memProf); err != nil {
	// 	panic(err)
	// }

	start := time.Now()
	// file, err := os.Open("./measurements.txt")
	// if err != nil {
	// 	panic(err.Error())
	// }
	// defer file.Close()

	// emptyBufRead(file)
	// emptyIoRead(file)
	//emptyBufRead(file)
	//concurrentEmptyRead("./measurements.txt")
	chunkedReadWithWorkerPool("./measurements.txt")
	// weather, cities := readWeatherDataBytes(file)
	// readFileTime := time.Now()
	// fmt.Printf("finished reading files in %f seconds\n", readFileTime.Sub(start).Seconds())

	// res := make(map[uint64]Temps)
	// analyzeTime := time.Now()
	// for city, temps := range weather {
	// 	var min int16 = 2555
	// 	var max int16 = -2555
	// 	var acc int64

	// 	for i := 0; i < len(temps); i++ {
	// 		if temps[i] < min {
	// 			min = temps[i]
	// 		}
	// 		if temps[i] > max {
	// 			max = temps[i]
	// 		}
	// 		acc += int64(temps[i])
	// 	}
	// 	t := Temps{float64(min / 10), float64(max / 10), float64(acc/int64(len(temps))) / 10}
	// 	res[city] = t
	// 	fmt.Printf("adding city %s with min %f max %f avg %f\n", cities[city], t.min, t.max, t.avg)
	// }

	// fmt.Printf("Analyzed the data in %f seconds\n", analyzeTime.Sub(readFileTime).Seconds())
	end := time.Now()
	fmt.Printf("finished in %v seconds", end.Sub(start).Seconds())
}

func concurrentEmptyRead(filename string) {
	fileSize := fileLength(filename)
	wg := sync.WaitGroup{}
	const chunks int = 32
	chunk := fileSize / int64(chunks)
	for i := 0; i < chunks; i++ {
		wg.Add(1)
		ix := i
		go func() {
			file, err := os.Open(filename)
			if err != nil {
				panic(err)
			}
			defer file.Close()
			offset := chunk * int64(ix)
			file.Seek(offset, 0)
			buf := make([]byte, chunk) //the chunk size
			n, _ := io.ReadFull(file, buf)
			log.Printf("Read %d bytes\n", n)
			wg.Done()
		}()
	}
	wg.Wait()
}

func chunkedReadWithWorkerPool(filename string) {
	const workers int = 8
	const chunkSize int = 4 * 1024 * 1024

	chunks := make(chan []byte, 100)
	res := make(chan int)
	wg := sync.WaitGroup{}
	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(chunks <-chan []byte, res chan<- int, w *sync.WaitGroup) {
			defer w.Done()
			length := 0
			reads := 0
			for in := range chunks {
				length += len(in)
				reads++
			}
			log.Printf("read %d bytes, in %d reads\n", length, reads)
			res <- length

		}(chunks, res, &wg)
	}

	go func() {
		wg.Wait()
		close(res)
	}()

	go func(filename string, chunks chan<- []byte) {
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		r := bufio.NewReader(file)
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
			chunks <- buf
		}

		close(chunks)
	}(filename, chunks)

	length := 0
	length += <-res
}

func fileLength(filename string) int64 {
	s, err := os.Stat(filename)
	if err != nil {
		panic(err)
	}
	size := s.Size()
	log.Printf("file size %d", size)
	return size
}

func emptyBufScanner(file io.Reader) {
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	length := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		length += len(line)
	}
	//log.Printf("Read %d bytes\n", length)
}

func emptyBufRead(file io.Reader) {
	length := 0
	r := bufio.NewReader(file)
	for {
		buf := make([]byte, 10*1024) //the chunk size
		n, err := r.Read(buf)        //loading chunk into buffer
		buf = buf[:n]
		length += n
		if n == 0 {

			if err != nil {
				fmt.Println(err)
				break
			}
			if err == io.EOF {
				break
			}
		}
	}
	log.Printf("Read %d bytes\n", length)
}

func emptyIoRead(file io.Reader) {
	b, err := io.ReadAll(file)

	if err != nil {
		panic(err)
	}
	log.Printf("Read %d bytes\n", len(b))
}

func readWeatherData(file io.Reader) map[string][]int16 {
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	// const maxCapacity = 2048 * 1024
	// buf := make([]byte, maxCapacity)
	// scanner.Buffer(buf, maxCapacity)
	weather := make(map[string][]int16)
	for scanner.Scan() {
		line := scanner.Text()
		city, temp := getDataAsInt(line)
		if _, exists := weather[city]; exists {
			weather[city] = append(weather[city], temp)
		} else {
			weather[city] = []int16{temp}
		}
	}

	return weather
}

func readWeatherDataBytes(file io.Reader) (map[uint64][]int16, map[uint64]string) {
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	cities := make(map[uint64]string)
	weather := make(map[uint64][]int16)
	i := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		cityHash := parseCityHash(line)
		temp, err := parseTemp(line)
		if err != nil {
			panic(err.Error())
		}

		if _, exists := cities[cityHash]; !exists {
			city := parseCityName(line)
			cities[cityHash] = city
			weather[cityHash] = make([]int16, 0, 1000)
		}
		weather[cityHash] = append(weather[cityHash], temp)
		if i%100000000 == 0 {
			fmt.Printf("%v processed another 100000000 lines\n", time.Now().GoString())
		}
		i++
	}

	return weather, cities
}

func readWeatherDataTwice(file io.Reader) (map[uint64][]int16, map[uint64]string) {
	readTwiceStart := time.Now()
	cityCounter := make(map[uint64]int)
	scn := bufio.NewScanner(file)
	for scn.Scan() {
		buffer := scn.Bytes()
		cityHash := parseCityHash(buffer)
		// if _, exists := cityCounter[cityName]; exists {
		// 	cityCounter[cityName]++
		// } else {
		// 	cityCounter[cityName] = 0
		// }
		cityCounter[cityHash]++
	}
	readTwiceEnd := time.Now()
	fmt.Printf("Time to read city count %f seconds\n", readTwiceEnd.Sub(readTwiceStart).Seconds())

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	cities := make(map[uint64]string)
	weather := make(map[uint64][]int16)
	i := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		cityHash := parseCityHash(line)
		temp, err := parseTemp(line)
		if err != nil {
			panic(err.Error())
		}

		if _, exists := cities[cityHash]; !exists {
			city := parseCityName(line)
			cities[cityHash] = city
			weather[cityHash] = make([]int16, 0, 1000)
		}
		weather[cityHash] = append(weather[cityHash], temp)
		if i%100000000 == 0 {
			fmt.Printf("%v processed another 100000000 lines\n", time.Now().GoString())
		}
		i++
	}

	return weather, cities
}

func parseCityName(buffer []byte) string {
	for i, b := range buffer {
		if b == ';' {
			return string(buffer[:i])
		}
	}
	return ""
}

func parseCityHash(buffer []byte) uint64 {
	hash := uint64(86425)
	for _, b := range buffer {
		if b == ';' {
			break
		}
		hash += uint64(b) + hash + hash<<5
	}
	return hash
	// return uint64(86421)
}

func parseTemp(buffer []byte) (int16, error) {
	isNegative := false
	temp := 0
	skip := true
	for _, b := range buffer {
		if skip && b != ';' {
			continue
		}
		if skip && b == ';' {
			skip = false
			continue
		}

		if b == '-' {
			isNegative = true
			continue
		}
		if b == '.' {
			continue
		}
		if b < '0' || b > '9' {
			return int16(0), fmt.Errorf("Invalid byte %c in buffer %s", b, string(buffer))
		}
		temp = temp*10 + int(b-'0')
	}
	if isNegative {
		temp = temp * -1
	}
	return int16(temp), nil
}

func getDataAsInt(entry string) (string, int16) {
	for i := 0; i < len(entry); i++ {
		if int(entry[i]) == 59 {
			v, err := strconv.ParseFloat(entry[i+1:], 64)
			if err != nil {
				panic(err.Error())
			}
			return entry[0:i], int16(v * 10)
		}
	}
	return "", 0
}
