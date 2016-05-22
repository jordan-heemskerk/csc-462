package main

import "os"
import "fmt"
import "mapreduce"
import "graphbuilder"
import "container/list"
import "regexp"
import "strings"
import "strconv"
import "log"
import "encoding/json"
import "github.com/fvbock/trie"

var pattern *regexp.Regexp

type NameTreePair struct {
    Total   int
    PrefixTree string
}

func Map(value string) *list.List {

    fs := strings.Split(value,"\n")

    var l = new(list.List)
    
    for _, v := range fs {

        // Slice with: [wholematch, "Name", "DoW", "HH", "MM"]
        extracted := pattern.FindAllStringSubmatch(v, -1)

        // check if we have a match            
        if len(extracted) > 0 {

            // this will be our key
            name := extracted[0][1]

            // Use DoW later
            // DoW := extracted[0][2]

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
            fmt.Printf("Empty line; Unable to parse a line of the file: " + v)
        }
    }
/*  for k, v := range counts {
        l.PushBack(mapreduce.KeyValue{Key: k, Value: strconv.Itoa(v)})
    }
*/
    
    return l
}

func Reduce(key string, values *list.List) string {
    // Create a new key result trie
    t := trie.NewTrie()
    total := 0

    // Build tree; add each hour and increase total message count
    for e := values.Front(); e != nil; e = e.Next() {
        total += 1
        t.Add(e.Value.(string))
    }

    // Key value pair to output; total number of messages and string dump of tree
    // The vdobler trie library does not support direct encoding/decoding of prefix trees
    // as such, we can't just store a dump of the tree in the struct. We write to a file to be read later
    treeFile := "00-" + key + "-Trie"
    t.DumpToFile(treeFile)
    ntp := NameTreePair{Total: total, PrefixTree: treeFile}

    encoded, err := json.Marshal(ntp)

    if err != nil {
        fmt.Println("Error: ",  err)
    }

    return string(encoded)
}

// Mapper can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
// Graphbuilder can be built using the base input file name via:
// 1) go run wc.go graph stack-hist x.txt 3
func main() {
    pattern = regexp.MustCompile("(.*);([\\w]*),[\\w]*,[\\w]*,[\\w]*,(\\d*):(\\d*)")

    switch len(os.Args) {
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
    case 5:
        // Graph building
        if os.Args[2] == "stack-hist" {
            graphbuilder.BuildGraph(os.Args[3], os.Args[4], os.Args[2])
        }
    default:
        fmt.Printf("%s: see usage comments in file\n", os.Args[0])
    }
}
