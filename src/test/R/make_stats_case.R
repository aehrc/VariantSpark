#!/usr/bin/env Rscript
#
# Generates Synthetic Datasets
# For regression testing
#
library(optparse)
library(ranger)

option_list = list(
    make_option(c("-p", "--prefix"), dest='filePrefix', default=NULL, help="Output file prexix [default= %default]"),
    make_option(c("-s", "--seed"), dest='seed', default=13, help="Random seed"),
    make_option(c("-l", "--labels"), dest='labels', default='labels', help="The labels to use [default= %default]"),
    make_option(c("-t", "--trees"), dest='nTrees', default=2000, help="The number of trees to build [default= %default]"),
    make_option(c("-r", "--repeats"), dest='nRepeats', default=50, help="The number of repeats [default= %default]")
)

opt_parser <- OptionParser(option_list=option_list)
params <- parse_args(opt_parser)

with(params, {

    if (!exists('filePrefix')) {
        print_help(opt_parser)
        stop("filePrefix is required", call.=FALSE)
    }

    print('Running with params:')
    print(as.matrix(params))

    dataFile <- sprintf('%s-std.csv', filePrefix)
    labelsFile <- sprintf('%s-%s.csv', filePrefix, labels)
    statsFile <- sprintf('%s-%s-stats.csv', filePrefix, labels)
    X_df <- read.csv(dataFile, header = TRUE, row.names = 1)
    labels_df <- read.csv(labelsFile, header = TRUE, row.names = 1)
    Xy_df = data.frame(X_df, label = as.factor(labels_df$cat2))

    nRepeats <- 50
    set.seed(seed)
    impSamples <- sapply(seq(from=1, length.out = nRepeats), function(n){ ranger(label ~ ., Xy_df, num.trees = nTrees, importance = 'impurity')$variable.importance})


    impSamples_mean <- apply(impSamples, MARGIN = 1, FUN=mean)
    impSamples_sd <- apply(impSamples, MARGIN = 1, FUN=sd)
    impSamplesStats <- data.frame(mean = impSamples_mean, sd = impSamples_sd)
    write.csv(impSamplesStats,statsFile, quote = FALSE)

#print(
#    ggplot(melt(as.data.frame(t(impSamples))), aes(x=variable, y=value)) + geom_boxplot()
#    + theme(axis.ticks.x=element_blank(), axis.text.x=element_blank())
#)

})


