## One Billion Row Challenge

### First Attempt

I just went for it and started coding. I used a 1 million line file to make the testing fast. I was in the flow. Read a line from the file with a `bufio.Scanner`, parse the line, put it in a map, after reading the file, combine all the results, and output it into a formatted string. Easy. Any fast. Less than two seconds for a million rows. 

I created the billion row file and ran against the big file. Then I waited for *minutes*. This was abviously not very good. I was hoping for under 30 seconds.

So I started changing things to see how that would affect. That started the downward spiral. I changed the maps to store a hash of the city name instead of the string. Then I changed the math to use integers instead of floats. The I started using `[]byte` instead of `string`. Somewhere along the way I introduced a logic error. Backing out wasn't getting me any closer. When I had something kinda working, I was sitting at about 180 seconds for 1 billion rows. 

### A New Beginning

This was more challenging than I originally planned. I found so many variables that I couldn't keep track of them.

Then I started writing this. I was going to write this when I completed the challenge, but right now felt like the right time. I need to organize my thoughts and different attempts.

### Second Attempt

There are a couple of major components to solving this.
- Read the file
- Process the data
- Format the output

In my first attempt I read the file line by line and started processing the data. In sequential operations. When I encountered slow code, it was difficult to determine where it was slow. 

This time my focus is reading the file as fast as possible. I called this *empty reading*. I only counted the bytes read. Nothing else.

#### Read file time
I am runing all of these benchmarks with cpu and memory profiling. This will add some overhead.
Read with buffered io scanning each line and returning text
```go
scanner := bufio.NewScanner(file)
scanner.Split(bufio.ScanLines)

length := 0
for scanner.Scan() {
    line := scanner.Text()
    length += len(line)
}
```
24 seconds

Read with buffered io scanning each line and returning bytes
```go
scanner := bufio.NewScanner(file)
scanner.Split(bufio.ScanLines)

length := 0
for scanner.Scan() {
    line := scanner.Bytes()
    length += len(line)
}
```
19 seconds

Swapping out the Split function to `bufio.ScanBytes` increased read time to 98 seconds. No good.

Read with a buffered reader with a 10 MB buffer
```go
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
```
Just a little over 9 seconds. Changing the buffer size didn't impact time to completion.

Concurrent Chunk Read

Splitting the chunked reads into 32 number or reads an doing each read concurrently on a different io.Reader
```go
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
```
This took about 23 seconds. Not any better


I wonder if this would work better if the reads were limited to a worker pool

Single chunked read with worker pool processing chunks

```go
func chunkedReadWithWorkerPool(filename string) {
	const workers int = 8
	const chunkSize int = 8 * 1024 * 1024

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
```
This one reads the entire file in about 9 seconds. I have two knobs that can be turned - number of workers and buffer size. I don't see much difference running between 4 and 32 workers. I sampled different buffer sizes from 4MB to 128MB. Anything from 4MB to 32MB produced a similar time to completion. This isn't different than the buffered reader attempt above. The other one didn't use goroutines or channels. I was hoping for an improvement. The time is almost identical, so maybe I can use the goroutines and channels to distribute the parsing later.

