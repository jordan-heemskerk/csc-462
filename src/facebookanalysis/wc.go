package main

import "os"
import "fmt"
import "mapreduce"
//import "graphbuilder"
import "container/list"
import "regexp"
import "strings"
import "strconv"
import "log"

var pattern *regexp.Regexp

func Map(value string) *list.List {

    fs := strings.Split(value,"\n")

    var l = new(list.List)
    
    for _, v := range fs {

        //Slice with: [wholematch, "Name", "DoW", "HH", "MM"]
        extracted := pattern.FindAllStringSubmatch(v, -1)

        //fmt.Printf("Extracted: " + extracted[0][0] + "\n")
        
        // check if we have a match            
        if len(extracted) > 0 {

            // this will be our key
            name := extracted[0][1]

            // Use DoW later
            //DoW := extracted[0][2]

            // Extract hour and minute strings
            HH := extracted[0][3]
            MM := extracted[0][4]

            // We need to do some parsing for hours to prefix a zero if necessary
            // ie. 0900
            iHH, err := strconv.Atoi(HH)
            
            if err != nil {
                log.Fatal("Not able to convert hour string to an integer")
            }

            sHH := fmt.Sprintf("%02d", iHH) 

            // build value
            v := sHH + MM

            l.PushBack(mapreduce.KeyValue{Key: name, Value: v})

        } else {

            fmt.Printf("Unable to parse a line of the file: " + v)

        }
    }
/*  for k, v := range counts {
        l.PushBack(mapreduce.KeyValue{Key: k, Value: strconv.Itoa(v)})
    }
*/
    
    return l
}


// you need to provide the code to iterate over list and add values
func Reduce(key string, values *list.List) string { 
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

// Mapper can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
// Build graph via
// 1) wc.go input.txt
func main() {
    pattern = regexp.MustCompile("(.*);([\\w]*),[\\w]*,[\\w]*,[\\w]*,(\\d*):(\\d*)")

    switch len(os.Args) {
    case 2:
        // Build graph
    //    graphbuilder.BuildGraph(os.Args[1])
    case 4:
        // Original implementation
        if os.Args[1] == "master" {
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
    default:
        fmt.Printf("%s: see usage comments in file\n", os.Args[0])
    }
}
