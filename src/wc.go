package main

import "os"
import "fmt"
import "mapreduce"
import "container/list"
import "regexp"
import "strings"
import "strconv"
//import "unicode"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents

var pattern *regexp.Regexp

// Each string is a line in the file
func Map(value string) *list.List {

    fs := strings.Split(value,"\n")

	var l = new(list.List)
	//counts := make(map[string]int)
	for _, v := range fs {
   	

        //Slice with: [wholematch, "DoW", "HH", "MM"]
        extracted := pattern.FindAllStringSubmatch(v, -1)

        if len(extracted) > 0 {

            //fmt.Println("%q\n", extracted)

            // Use DoW later
            //DoW := extracted[0][1]
            HH := extracted[0][2]
            MM := extracted[0][3]

            iHH, _ := strconv.Atoi(HH) // TODO err
            sHH := fmt.Sprintf("%02d", iHH)

            k := sHH + MM

            

            l.PushBack(mapreduce.KeyValue{Key: k, Value: "1"})
        }
	}
/*	for k, v := range counts {
		l.PushBack(mapreduce.KeyValue{Key: k, Value: strconv.Itoa(v)})
	}
*/
    
	// you must determine what this function should reddturn! :)
	return l
}


// you need to provide the code to iterate over list and add values
func Reduce(key string, values *list.List) string { 

    // initialize total to 0
    // for every value in the list
    //     convert the value to an integer
    //             check for an error! :) 
    //     add the integer value to the total
    // return the total

	var total = 0
	
	for e := values.Front(); e != nil; e = e.Next() {
       
        if val, err := strconv.Atoi(e.Value.(string)); err == nil { 
            total += val
        } else {
            fmt.Printf("Error converting the interface to an integer\n")
        }
	}

    return strconv.Itoa(total) 

}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
     	fmt.Println("Welcome to my MapReduce!");

    pattern = regexp.MustCompile("([\\w]*),[\\w]*,[\\w]*,[\\w]*,(\\d*):(\\d*)")

	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}
