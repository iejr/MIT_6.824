package mapreduce

import (
    "fmt"
    "hash/fnv"
    "io/ioutil"
    "encoding/json"
    "encoding/gob"
    "bytes"
    b64 "encoding/base64"
    // "strconv"
)

func doMap(
    jobName string, // the name of the MapReduce job
    mapTask int, // which map task this is
    inFile string,
    nReduce int, // the number of reduce task that will be run ("R" in the paper)
    mapF func(filename string, contents string) []KeyValue,
) {
    //
    // doMap manages one map task: it should read one of the input files
    // (inFile), call the user-defined map function (mapF) for that file's
    // contents, and partition mapF's output into nReduce intermediate files.
    //
    // There is one intermediate file per reduce task. The file name
    // includes both the map task number and the reduce task number. Use
    // the filename generated by reduceName(jobName, mapTask, r)
    // as the intermediate file for reduce task r. Call ihash() (see
    // below) on each key, mod nReduce, to pick r for a key/value pair.
    //
    // mapF() is the map function provided by the application. The first
    // argument should be the input file name, though the map function
    // typically ignores it. The second argument should be the entire
    // input file contents. mapF() returns a slice containing the
    // key/value pairs for reduce; see common.go for the definition of
    // KeyValue.
    //
    // Look at Go's ioutil and os packages for functions to read
    // and write files.
    //
    // Coming up with a scheme for how to format the key/value pairs on
    // disk can be tricky, especially when taking into account that both
    // keys and values could contain newlines, quotes, and any other
    // character you can think of.
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
    //
    // Your code here (Part I).
    //
    file_body, read_err := ioutil.ReadFile(inFile);
    if read_err != nil {
        panic(read_err)
    }

    // file_body = []byte("123")
    key_value := mapF(inFile, string(file_body))
    // fmt.Println("[map]key_value[0].value length = " + strconv.Itoa(len(key_value[0].Value)))
    // return

    stat_dict := make(map[int] []KeyValue)
    for i := 0; i < len(key_value); i++ {
        reduce_id := ihash(key_value[i].Key) % nReduce

        bucket, key_exist := stat_dict[reduce_id]
        if key_exist == false{
            bucket = make([]KeyValue, 0)
        }

        stat_dict[reduce_id] = append(bucket, key_value[i])
    }

    for reduce_id, kv_list := range stat_dict {
        reduce_filename := reduceName(jobName, mapTask, reduce_id)
        // reduce_filename = "debug_reduce_filename"
        fmt.Println("[map]write file: " + reduce_filename)

        // temp := kv_list[0].Value
        // json_text, encode_err := json.Marshal(temp)

        enc_list := make([]string, len(kv_list))
        for i := 0; i < len(kv_list); i++ {
            // key := kv_list[i].Key
            // val := kv_list[i].Value
            // fmt.Println("[map]reduce_id: " + strconv.Itoa(reduce_id) + " key: " + key + " len(val): " + strconv.Itoa(len(val)))

            temp_buf := bytes.Buffer{}
            enc := gob.NewEncoder(&temp_buf)
            enc_err := enc.Encode(kv_list[i])
            if enc_err != nil {
                panic(enc_err)
            }
            enc_list[i] = b64.StdEncoding.EncodeToString(temp_buf.Bytes())
        }

        json_text, encode_err := json.Marshal(enc_list)
        if encode_err != nil {
            panic(encode_err)
        }

        write_err := ioutil.WriteFile(reduce_filename, []byte(json_text), 0644)
        if write_err != nil {
            panic(write_err)
        }

        // panic("stop!!!!")
    }
}

func ihash(s string) int {
    h := fnv.New32a()
    h.Write([]byte(s))
    return int(h.Sum32() & 0x7fffffff)
}
