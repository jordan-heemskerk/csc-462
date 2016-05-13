package graphbuilder

import (
    "bufio"
    "image"
    "image/color"
    "image/draw"
    "image/png"
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

func BuildGraph(fileName string) {
    // parse file on new lines
    valuesMap := splitter(fileName)

    // construct trie
    fbtrie := trie.NewTrie()

    // build tree
    for key, value := range valuesMap {
        for i:=0; i<value; i++ {
          fbtrie.Add(key)
        }
    }

    //
    //  Plot the pie chart!
    //
    piec := chart.PieChart{Title:"Chatty Cathy"}

    dumper := NewDumper("xpie1", 2, 2, 1000, 500)
    defer dumper.Close()

    // TODO - Later - Traverse the ENTIRE tree (down to the minutes; via range)
    // var members = fbtrie.Members()

    hcollection := []string{}
    vcollection := []float64{}

    // loop: go through all 24 hours; get string version of integer
    for a:= 0; a < 24; a++ {
      var str string

      if a < 10 {
        str = fmt.Sprintf("0%v", a)
      } else {
        str = fmt.Sprintf("%v", a)
      }

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

