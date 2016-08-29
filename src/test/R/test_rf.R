library(randomForest)

make_data <- function(nVars, nSamples) {
    sapply(1:nVars, function(t){sample(0:2, nSamples, replace=TRUE)})
}


X<- make_data(1000,1000)
#y<- ((X[,2] - 1)*2*0.5 + (X[,5] - 1)*2*0.5 + + rnorm(length(X[,2]), sd=2.0)) >= 0.5
y <- ((X[,10])*(X[,11])*2.0 + rnorm(length(X[,2]), sd=1.0)) > 1.0
rf <- randomForest(X,as.factor(y), importance = TRUE)
rf
varImpPlot(rf)