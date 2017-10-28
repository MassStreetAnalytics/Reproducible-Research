#Spark UI can be opened with http://localhost:4040

# Set wd to directory of source file.
# This only works in R studio.
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

#Install spark and sparklyr if neccessary.
#devtools::install_github("rstudio/sparklyr")
#spark_install("2.2.0")
library(tidyverse)
library(sparklyr)
library(replyr) #This package has a dependency on wrapr
sc = spark_connect(master="local") #This is NOT a Spark Context!

# Sparklyr Cheat Sheet Walk Through

iris_tbl <- copy_to(sc, iris)

set.seed(100)


#Split Iris Data

#Split the iris data into test/train sets
#Register the training set
#Create an R reference object for the training set
import_iris = copy_to(sc,iris,"spark_iris", overwrite=TRUE)

partition_iris <- sdf_partition(import_iris, training=0.5, testing=0.5)

sdf_register(partition_iris, c("spark_iris_training", "spark_iris_test"))

tidy_iris <- tbl(sc, "spark_iris_training") %>% select(Species, Petal_Length, Petal_Width)


### Build and Train a Model in Spark


model_iris <- tidy_iris %>% ml_decision_tree(response="Species", features=c("Petal_Length", "Petal_Width"))


### Test the Model


test_iris <- tbl(sc, "spark_iris_test")

pred_iris <- sdf_predict(model_iris, test_iris) %>% collect


### Visualize the Model Prediction


library(ggplot2)

pred_iris %>%inner_join(data.frame(prediction=0:2, lab=model_iris$model.parameters$labels)) %>%ggplot(aes(Petal_Length, Petal_Width, col=lab)) + geom_point()


#Evaluate the Model Prediction

#Create an evaluation response column in the data


pred_iris2 <- pred_iris %>%
mutate(predSpecies = ifelse(test = (prediction == 1), yes = "setosa", no = ifelse(test = (prediction == 0), yes = "virginica",
                                                                                    no = "versicolor"))) %>%
  mutate(correct = ifelse(test = (predSpecies == Species), yes = 1, no = 0)) %>%
  collect


#Visualize labeled data


pred_iris2 %>% ggplot(aes(Petal_Length, Petal_Width, col=correct)) + geom_point()






spark_disconnect(sc)

