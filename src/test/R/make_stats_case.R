#!/usr/bin/env Rscript
library(optparse)
library(ggplot2)
library(reshape2)
library(ranger)

nTrees <- 1000

prefix <- '../../test/data/stats/stats_100_1000_cont_0.0'
dataFile <- sprintf('%s-std.csv', prefix)
labelsFile <- sprintf('%s-labels_null.csv', prefix)
X_df <- read.csv(dataFile, header = TRUE, row.names = 1)
labels_df <- read.csv(labelsFile, header = TRUE, row.names = 1)
Xy_df = data.frame(X_df, label = as.factor(labels_df$cat2))

nRepeats <- 50
set.seed(13)
impSamples <- sapply(seq(from=1, length.out = nRepeats), function(n){ ranger(label ~ ., Xy_df, num.trees = nTrees, importance = 'impurity')$variable.importance})

print(
    ggplot(melt(as.data.frame(t(impSamples))), aes(x=variable, y=value)) + geom_boxplot()
    + theme(axis.ticks.x=element_blank(), axis.text.x=element_blank())
)


impSamples_mean <- apply(impSamples, MARGIN = 1, FUN=mean)
impSamples_sd <- apply(impSamples, MARGIN = 1, FUN=sd)
impSamplesStats <- data.frame(mean = impSamples_mean, sd = impSamples_sd)
write.csv(impSamplesStats,'../../test/data/stats/stats_100_1000_cont_0.0-stats.csv', quote = FALSE)




