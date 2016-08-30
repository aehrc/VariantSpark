library(boot)
library(randomForest)

make_data <- function(nVars, nSamples) {
    sapply(1:nVars, function(t){sample(0:2, nSamples, replace=TRUE)})
}


X<- make_data(1000,1000)

make_response <- function(X, w, sigma) {
    as.factor(inv.logit((as.matrix(X)-1) %*% (w) + rnorm(ncol(X), sd=sigma)) > 0.5)
}

y <- make_response(X, c(2,2,rep(0.0,length.out=1000-2)), 1.0)

rf <- randomForest(X,y, importance = TRUE)
rf
varImpPlot(rf)