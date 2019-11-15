package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	file, err := os.OpenFile(inFile, os.O_RDONLY, 0666)
	if err != nil {
		return
	}
	defer file.Close()
	var content string
	var buf [4096]byte
	reader := bufio.NewReader(file)
	//从指定文件读取内容，交给用户指定的map函数处理，生成对应的map对（从数据源提取数据）操作粒度是文件->map组
	//将map对根据指定的规则，分成指定的reduce任务个数
	//每个reduce对自己指定的任务，进行文件整合(分散的结果）（分析数据，生成结果），操作粒度是每个key
	//master合并所有结果
	for {
		n, err := reader.Read(buf[:])
		if err == io.EOF {
			if n != 0 {
				content += string(buf[0:n])
			}
			break
		}
		content += string(buf[0:n])
	}
	kvs := mapF(inFile, content)
	fm := make(map[string]*os.File)

	for _, kv := range kvs {
		reduceNum := int(ihash(kv.Key)) % nReduce
		reduceFileName := reduceName(jobName, mapTaskNumber, reduceNum)
		file := fm[reduceFileName]

		if file == nil {
			file, err = os.OpenFile(reduceFileName, os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				fmt.Println(err)
				return
			}
			fm[reduceFileName] = file
			//defer file.Close()
		}
		enc := json.NewEncoder(file)
		enc.Encode(kv)

	}
	for _, rfile := range fm {
		rfile.Close()
	}

	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
}

//ihash 决定 key 属于哪个文件。。。
// ihash(key) / nReduce = file num(文件编号)
//比如5个文件， 3个nreduce
//产生的文件编码为： 1-0,1-1,1-2,...,4-0 4-1 4-2
//reduce根据可能的map编号(0-5)，依次去访问自己的reduce编号（0,1,2其中一个)，一堆reduce组装5个文件
//类比word count
func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
