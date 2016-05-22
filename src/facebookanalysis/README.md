# Distributed Systems Facebook Analysis

This project....


## Structure
```
├── facebookanalysis
│   ├── fbparser
│   │   ├── fbparser.py
│   │   ├── messages.htm
│   │   ├── README.md
│   │   └── requirements.txt
│   ├── README.md
│   └── wc.go
├── graphbuilder
│   └── graphbuilder.go
└── mapreduce
    ├── common.go
    ├── mapreduce.go
    ├── master.go
    ├── test_test.go
    └── worker.go
```


## Useage:

1. Preprocess your facebook data to create input data file; see ```fbparser/README```

2. Use MapReduce to create prefix trees of all your conversations; One file will be produced per person. Run ```go run wc.go master x.txt sequential```

3. Generate a stacked bar graph via: ```go run wc.go graph stack-hist mrtmp.x.txt <number of result files>```


## Notes:

* wc.go is modified from the original files provided in UVic's CSC 462's Cource
