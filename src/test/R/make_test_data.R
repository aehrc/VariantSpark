#!/usr/bin/env Rscript

#
# Creates test data for decsion tree comparision test agains R implementation 
#

library(rpart)

data <- read.csv('../../../data/CNAE-9.csv', header = FALSE)
colNames <- sapply(0:(ncol(data)-2), FUN = function(i) {paste('v', i, sep='')})
colnames(data) <- c('category', colNames)
treeModel4 <- rpart(as.factor(category) ~ ., data, method = 'class', control = rpart.control(minsplit = 2, minbucket = 1, cp=0.0, maxdepth=4, maxsurrogate = 0, maxcompete = 0))
treeModel15 <- rpart(as.factor(category) ~ ., data, method = 'class', control = rpart.control(minsplit = 2, minbucket = 1, cp=0.0, maxdepth=15, maxsurrogate = 0, maxcompete = 0))
treeModel30 <- rpart(as.factor(category) ~ ., data, method = 'class', control = rpart.control(minsplit = 2, minbucket = 1, cp=0.0, maxdepth=30, maxsurrogate = 0, maxcompete = 0))

predictions <- data.frame(maxdepth_4  = predict(treeModel4, type = "vector"),
                          maxdepth_15  = predict(treeModel15, type = "vector"),
                          maxdepth_30  = predict(treeModel30, type = "vector")
                          )

importances <-data.frame(row.names=colNames)
importances[names(treeModel4$variable.importance), 'maxdepth_4'] <- treeModel4$variable.importance
importances[names(treeModel15$variable.importance), 'maxdepth_15'] <- treeModel15$variable.importance
importances[names(treeModel30$variable.importance), 'maxdepth_30'] <- treeModel30$variable.importance
importances[is.na(importances)] <- 0.0


write.csv(predictions, '../../../src/test/data/CNAE-9_R_predictions.csv', quote=FALSE)
write.csv(importances, '../../../src/test/data/CNAE-9_R_importance.csv', quote=FALSE)


