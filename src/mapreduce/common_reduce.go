package mapreduce

import (
	"encoding/json"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	var kv KeyValue

	//TODO 需要读取nmap个文件，然后整合nmap个文件中key值相同的kv，将他们value组成values数组
	ksm := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		file, _ := os.OpenFile(fileName, os.O_RDONLY, 0666)
		defer file.Close()
		dec := json.NewDecoder(file)
		for dec.Decode(&kv) == nil {
			vs := ksm[kv.Key]
			if vs == nil {
				vs = []string{kv.Value}
				ksm[kv.Key] = vs
			} else {
				ksm[kv.Key] = append(vs, kv.Value)
			}

		}
	}
	//create merge file
	mergeFileName := mergeName(jobName, reduceTaskNumber)
	mergeFile, _ := os.OpenFile(mergeFileName, os.O_APPEND|os.O_CREATE, 0666)

	enc := json.NewEncoder(mergeFile)
	for k, vs := range ksm {
		kvi := KeyValue{k, reduceF(k, vs)}
		enc.Encode(kvi)
	}
	mergeFile.Close()
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
}
