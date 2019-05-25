package mapreduce

import (
    "fmt"
    "os"
    // "hash/fnv"
    "io/ioutil"
    "encoding/json"
    "encoding/gob"
    "bytes"
    b64 "encoding/base64"
    "strconv"
)


func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
        stat_dict := make(map[string] []string)
        for i := 0; i < nMap; i++ {
            reduce_filename := reduceName(jobName, i, reduceTask)
            fmt.Println("[reduce]read file: " + reduce_filename)
            file_body, read_err := ioutil.ReadFile(reduce_filename)
            if read_err != nil {
                fmt.Println("[reduce]read file error!")
                continue
            }

            var decode_json []string
            json.Unmarshal(file_body, &decode_json)
            fmt.Println("[reduce]len(decode_json): " + strconv.Itoa(len(decode_json)))

            kv_list := make([]KeyValue, len(decode_json))
            for j := 0; j < len(decode_json); j++ {
                bytes_seq, dec_err := b64.StdEncoding.DecodeString(decode_json[j])
                if dec_err != nil {
                    panic(dec_err)
                }

                temp_buf := bytes.Buffer{}
                temp_buf.Write(bytes_seq)

                dec := gob.NewDecoder(&temp_buf)
                dec_err = dec.Decode(&(kv_list[j]))
                if dec_err != nil {
                    panic(dec_err)
                }
            }

            for j := 0; j < len(kv_list); j++ {
                key := kv_list[j].Key
                val := kv_list[j].Value

                // fmt.Println("[reduce]key: " + key + " len(val): " + strconv.Itoa(len(val)))

                bucket, key_exist := stat_dict[key]
                if key_exist == false {
                    bucket = make([]string, 0)
                }

                stat_dict[key] = append(bucket, val)
            }
        }

        fp, _ := os.Create(outFile)
        fmt.Println("[reduce]write file: " + outFile)
        enc := json.NewEncoder(fp)
        for key, list := range stat_dict {
            reduce_ret := reduceF(key, list)
            enc.Encode(KeyValue{key, reduce_ret})
        }
        fp.Close()
}
