# Facebook Analysis

└── src
    ├── facebookanalysis
    │   ├── fbparser.py
    │   ├── input_00.txt
    │   └── wc.go
    ├── graphbuilder
    │   └── graphbuilder.go
    └── mapreduce
        ├── common.go
        ├── mapreduce.go
        ├── master.go
        └── worker.go


## Create a pie chart of your message history

1. Go to facebook and download your data: account/settings
2. Build and install go packages
3. Run fbparser.py on your messages.htm file to create input_xx.txt
4. Run wc.go master input_xx.txt sequential . This creates an aggregate count file, mrtmp.input_00.txt
5. Run wc.go mrtmp.input_00.txt . This will create a pie chart image.
