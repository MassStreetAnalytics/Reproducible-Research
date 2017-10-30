#Spark UI can be opened with http://localhost:4040

# Set wd to directory of source file.
# This only works in R studio.
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

#This is an issue. There has to be a cleaner way to
#manage script dependencies
if (!require("sparklyr")) {
  install.packages("sparklyr")  
}

if (!require("tidyverse")) {
  install.packages("tidyverse")  
}

#Install spark and sparklyr if neccessary.
#devtools::install_github("rstudio/sparklyr")
#spark_install("2.2.0")
library(tidyverse)
library(sparklyr)
sc = spark_connect(master="local") #This is NOT a Spark Context!



set.seed(100)



tbl_import_iris = spark_read_csv(sc, path = "../../Data/iris/iris_dataset.csv", col_names = TRUE, name = "import_iris", overwrite = TRUE)

#Split the iris data into test/train sets
#Register the training set
#Create an R reference object for the training set
partition_iris <- sdf_partition(tbl_import_iris, training=0.5, testing=0.5) #sdf_partition is not actually a partition

#this is all done with SparkSQL so you have to give things a table name
sdf_register(partition_iris, c("spark_iris_training", "spark_iris_test")) 

tidy_iris <- tbl(sc, "spark_iris_training") %>% select(Species, PetalLength, PetalWidth)


### Build and Train a Model in Spark


model_iris <- tidy_iris %>% ml_decision_tree(response="Species", features=c("PetalLength", "PetalWidth"))


### Test the Model


test_iris <- tbl(sc, "spark_iris_test")

pred_iris <- sdf_predict(model_iris, test_iris) %>% collect


### Visualize the Model Prediction


pred_iris %>%inner_join(data.frame(prediction=0:2, lab=model_iris$model.parameters$labels)) %>%ggplot(aes(PetalLength, PetalWidth, col=lab)) + geom_point()

#You can end here or keep going.
#spark_disconnect(sc) 

#Evaluate the Model Prediction
#Create an evaluation response column in the data
#This is more to demonstrate the dplyr mutate function
#mutate lets you add new variables and preserves existing vars


pred_iris2 <- pred_iris %>%
mutate(predSpecies = ifelse(test = (prediction == 1), yes = "setosa", no = ifelse(test = (prediction == 0), yes = "virginica", no = "versicolor"))) %>%
mutate(correct = ifelse(test = (predSpecies == Species), yes = 1, no = 0)) %>%
collect


#Visualize labeled data


pred_iris2 %>% ggplot(aes(PetalLength, PetalWidth, col=correct)) + geom_point()





#Clean Up
spark_disconnect(sc)
rm(list=ls())

