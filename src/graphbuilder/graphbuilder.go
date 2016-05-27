package graphbuilder

import (
    "bufio"
    "image"
    "image/color"
    "image/draw"
    "image/png"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "strconv"
    "strings"

    "github.com/fvbock/trie"
    "github.com/ajstarks/svgo"
    "github.com/vdobler/chart"
    "github.com/vdobler/chart/imgg"
    "github.com/vdobler/chart/svgg"
    "github.com/vdobler/chart/txtg"
)

type ParsedPair struct {
  Key   string
  Trie NameTreePair
}

type FilePair struct {
  Key string
  Value string
}

type NameTreePair struct {
    Total   int
    PrefixTree string
}


// -------------------------------------------------------------------------
// Dumper

// Dumper helps saving plots of size WxH in a NxM grid layout in several formats
type Dumper struct {
  N, M, W, H, Cnt           int
  S                         *svg.SVG
  I                         *image.RGBA
  svgFile, imgFile, txtFile *os.File
}

func NewDumper(name string, n, m, w, h int) *Dumper {
  var err error
  dumper := Dumper{N: n, M: m, W: w, H: h}

  dumper.svgFile, err = os.Create(name + ".svg")
  if err != nil {
    panic(err)
  }
  dumper.S = svg.New(dumper.svgFile)
  dumper.S.Start(n*w, m*h)
  dumper.S.Title(name)
  dumper.S.Rect(0, 0, n*w, m*h, "fill: #ffffff")

  dumper.imgFile, err = os.Create(name + ".png")
  if err != nil {
    panic(err)
  }
  dumper.I = image.NewRGBA(image.Rect(0, 0, n*w, m*h))
  bg := image.NewUniform(color.RGBA{0xff, 0xff, 0xff, 0xff})
  draw.Draw(dumper.I, dumper.I.Bounds(), bg, image.ZP, draw.Src)

  dumper.txtFile, err = os.Create(name + ".txt")
  if err != nil {
    panic(err)
  }

  return &dumper
}

func (d *Dumper) Close() {
  png.Encode(d.imgFile, d.I)
  d.imgFile.Close()

  d.S.End()
  d.svgFile.Close()

  d.txtFile.Close()
}

func (d *Dumper) Plot(c chart.Chart) {
  row, col := d.Cnt/d.N, d.Cnt%d.N

  igr := imgg.AddTo(d.I, col*d.W, row*d.H, d.W, d.H, color.RGBA{0xff, 0xff, 0xff, 0xff}, nil, nil)
  c.Plot(igr)

  sgr := svgg.AddTo(d.S, col*d.W, row*d.H, d.W, d.H, "", 12, color.RGBA{0xff, 0xff, 0xff, 0xff})
  c.Plot(sgr)

  tgr := txtg.New(100, 30)
  c.Plot(tgr)
  d.txtFile.Write([]byte(tgr.String() + "\n\n\n"))

  d.Cnt++

}
// -------------------------------------------------------------------------

func splitter(fileName string) map[string]int {
  infile, err := os.Open(fileName)

  if err != nil {  
      log.Fatal("Split: ", err)
  }

  defer infile.Close()

  values := make(map[string]int)

  scanner := bufio.NewScanner(infile)

  for scanner.Scan() {
      line := strings.Split(scanner.Text(), ": ")
      key := line[0]
      count, _ := strconv.Atoi(line[1])
      values[key] = count
  }

  return values
}

func Itohour(h int) string{
  var hour string

  if h < 10 {
    hour = fmt.Sprintf("0%v", h)
  } else {
    hour = fmt.Sprintf("%v", h)
  }

  return hour  
}

// -------------------------------------------------------------------------


func BakePie(fileName string) {
  // TODO: fix Donut Implementation
  //  Plot the pie chart!
  
  fbtrie := trie.NewTrie()

  piec := chart.PieChart{Title:"Chatty Cathy"}

  graphName := fileName + "pie"

  dumper := NewDumper(graphName, 2, 2, 1000, 500)
  defer dumper.Close()

  // TODO - Later - Traverse the ENTIRE tree (down to the minutes; via range)
  // var members = fbtrie.Members()

  hcollection := []string{}
  vcollection := []float64{}

  // loop: go through all 24 hours; get string version of integer
  for a:= 0; a < 24; a++ {
    str := Itohour(a)

    // get counts for the hour from tree
    _, count := fbtrie.HasPrefixCount(str)

    // Build data pairs at same time
    hcollection = append(hcollection, str)
    vcollection = append(vcollection, float64(count))      
  }

  // Add pairs to the pie chart
  piec.AddDataPair("Hour", hcollection, vcollection)

  // Configure the pie...
  piec.Data[0].Samples[0].Flag = true
  piec.Inner = 0.5
  piec.Key.Border = -1
  piec.FmtVal = chart.AbsoluteValue

  fmt.Println("Plotting....")
  dumper.Plot(&piec)

  fmt.Println("Done")
}

func TallyHourlyMessages(item ParsedPair) []float64{
  itemTrie, _ := trie.LoadFromFile(item.Trie.PrefixTree)

  var hourList []float64
  for h := 0; h < 24; h++ {
    hour := Itohour(h)
    // get number of messages from tree
    _, tally := itemTrie.HasPrefixCount(hour)

    hourList = append(hourList, float64(tally))
  }

  return hourList
}

func BuildStackedHistogram(first ParsedPair, second ParsedPair, third ParsedPair) {
  graphName := "Fabulous Facebook Friends"
  dumper := NewDumper(graphName, 2, 2, 1000, 500)
  defer dumper.Close()

  hist := chart.BarChart{Title: graphName, Stacked: true}
  hist.XRange.Label = "Hour of Day"
  hist.YRange.Label = "Number of Messages"

  red := chart.Style{LineColor: color.NRGBA{0xff, 0x00, 0x00, 0xff}, LineWidth: 1, FillColor: color.NRGBA{0xff, 0x80, 0x80, 0xff}}
  green := chart.Style{LineColor: color.NRGBA{0x00, 0xff, 0x00, 0xff}, LineWidth: 1, FillColor: color.NRGBA{0x80, 0xff, 0x80, 0xff}}
  blue := chart.Style{LineColor: color.NRGBA{0x00, 0x00, 0xff, 0xff}, LineWidth: 1, FillColor: color.NRGBA{0x80, 0x80, 0xff, 0xff}}

  var hours = []float64{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23}

  firstName := first.Key + " : " + strconv.Itoa(first.Trie.Total)
  secondName := second.Key + " : " + strconv.Itoa(second.Trie.Total)
  thirdName := third.Key + " : " + strconv.Itoa(third.Trie.Total)

  firstTally := TallyHourlyMessages(first)
  secondTally := TallyHourlyMessages(second)
  thirdTally := TallyHourlyMessages(third)

  hist.AddDataPair(firstName,  hours, firstTally, red)
  hist.AddDataPair(secondName, hours, secondTally, blue)
  hist.AddDataPair(thirdName,  hours, thirdTally, green)

  dumper.Plot(&hist)
}

func BuildGraph(baseFileName string, nFiles string, graphType string) {

    var firstPair, secondPair, thirdPair ParsedPair

    NFiles, _ := strconv.Atoi(nFiles)

    for i:=0; i < NFiles; i++ {
      n := strconv.Itoa(i)

      fileName := baseFileName + "-res-" + n

      // open and read results file
      file, _ := os.Open(fileName)
      defer file.Close()

      scanner := bufio.NewScanner(file)
      var line string

      for scanner.Scan() {
        line = scanner.Text()

        var curr ParsedPair
        var fp FilePair
        var ntp NameTreePair
        
        // read line: {Key, Value} pair
        _ = json.Unmarshal([]byte(line), &fp)
        curr.Key = fp.Key

        // read value's name tree pair 
        _ = json.Unmarshal([]byte(fp.Value), &ntp)
        curr.Trie = ntp

        // compare and determine if current trie is within (ordered) top 3
        if curr.Trie.Total > firstPair.Trie.Total {
          firstPair = curr
        } else if curr.Trie.Total > secondPair.Trie.Total {
          secondPair = curr
        } else if curr.Trie.Total > thirdPair.Trie.Total {
          thirdPair = curr
        }
      }
    }

    // TODO: Route graph construction based on graphType
    BuildStackedHistogram(firstPair, secondPair, thirdPair)
    // BakePie(baseFileName)
}
