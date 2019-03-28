library(rpart)
library(randomForest)
library(ggplot2)
library(reshape2)
library(ranger)


nVariables <- 100
nSamples <- 1000
nTrees <- 5000



#create a randomized input data

varNames <- paste0('v_', seq(from=0, length.out = nVariables))

set.seed(13)
X<-matrix(runif(nVariables * nSamples), nrow = nSamples, ncol = nVariables)
colnames(X)<-varNames

set.seed(17)
weights <- rnorm(nVariables)
names(weights) <- varNames
y <- X %*% weights
y_cont <- y[,1]
y_bi <-as.numeric(y_cont>=0)

X_df = data.frame(as.data.frame(X), label = as.factor(y_bi))

write.csv(X,'../../test/data/data.csv', quote = FALSE)
write.csv(data.frame(cont=y_cont, label=y_bi),'../../test/data/data-labels.csv', quote = FALSE)

nRepeats <- 10
set.seed(13)
#impSamples <- sapply(seq(from=1, length.out = nRepeats), function(n){randomForest(X,y_bi, importance= TRUE, ntree = nTrees)$importance[,4]})
impSamples <- sapply(seq(from=1, length.out = nRepeats), function(n){ ranger(label ~ ., X_df, num.trees = nTrees, importance = 'impurity')$variable.importance})

print(
    ggplot(melt(as.data.frame(t(impSamples))), aes(x=variable, y=value)) + geom_boxplot()
)
