library(RUnit)
library(dplyr)
library(TeradataAsterR)
library(modelFactoryR)
library(methods)

getTaConnection()

test.suite <- defineTestSuite("example",dirs = file.path("tests"),testFileRegexp = '^\\d+\\.R')

test.result <- runTestSuite(test.suite)

printTextProtocol(test.result)

