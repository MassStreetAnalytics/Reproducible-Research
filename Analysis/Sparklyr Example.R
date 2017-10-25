#Spark UI can be opened with http://localhost:4040

# Set wd to directory of source file.
# This only works in R studio.
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

#Install spark and sparklyr if neccessary.
#devtools::install_github("rstudio/sparklyr")
#spark_install("2.2.0")
library(tidyverse)
library(sparklyr)
library(Amelia)
library(replyr)
sc = spark_connect(master="local") #This is NOT a Spark Context!

titanic_train = spark_read_csv(sc, name="titanic_train", path="../Data/titanic_train.csv", header = TRUE, delimiter = ",", quote = "\"", escape = "\\", charset = "UTF-8", null_value = NULL, repartition = 0, memory = TRUE, overwrite = TRUE)

replyr_summary(titanic_train)

spark_disconnect(sc)

