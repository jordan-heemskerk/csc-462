#!/bin/bash

go test | tee out
grep FAIL out
