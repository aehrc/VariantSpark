#!/usr/bin/env Rscript
library(optparse)
option_list = list(
    make_option(c("-p", "--prefix"), dest='filePrefix', default=NULL, help="Output file prexix [default= %default]"),
    make_option(c("-r", "--seed"), dest='seed', default=13, help="Random seed"),
    make_option(c("-v", "--no-vars"), dest='nVariables', default=2000, help="Number od variables [default= %default]"),
    make_option(c("-s", "--no-samples"), dest='nSamples', default=500, help="Number od samples [default= %default]"),
    make_option(c("-m", "--max-value"), dest='maxValue', default=1.0, help="Max value [default= %default]"),
    make_option(c("-t", "--sparsity"), dest='sparsity', default=0.0, help="Sparsity [default= %default]"),
    make_option(c("-f", "--factorize"), dest='factorize', default=FALSE, action='store_true', help="Factorize [default= %default]")
)

opt_parser <- OptionParser(option_list=option_list)
params <- parse_args(opt_parser)

with(params, {

    if (!exists('filePrefix')) {
        print_help(opt_parser)
        stop("filePrefix is required", call.=FALSE)
    }

    if (factorize && maxValue < 2) {
        stop("Factorization requested but max values it less than 2. Will result in all zeros")
    }

    print('Running with params:')
    print(as.matrix(params))

    fileName <- function(suffix) {
        paste(filePrefix, suffix, sep = '-')
    }

    write.table(as.matrix(params), fileName('meta.txt'), col.names = FALSE, sep = ':', quote = FALSE)

    #create a randomized input data
    varFormat <- sprintf('v_%%0%dd', floor(log10(nVariables-1)) + 1)
    varNames <- sprintf(varFormat, seq(from=0, length.out = nVariables))
    sampleFormat <- sprintf('s_%%0%dd', floor(log10(nSamples-1)) + 1)
    sampleNames <- sprintf(sampleFormat, seq(from=0, length.out = nSamples))

    #Generate Initial Variables
    set.seed(seed)
    data <- runif(nVariables * nSamples) * (runif(nVariables * nSamples) >= sparsity)
    X_raw<-matrix(data, nrow = nSamples, ncol = nVariables)
    colnames(X_raw)<-varNames
    rownames(X_raw)<-sampleNames

    # apply distortion
    X_dist <- X_raw #(exp(distort*X_raw)-1)/(exp(distort)-1)

    X <- if (factorize) floor(X_dist * maxValue) else X_dist * maxValue

    write.csv(X,fileName('std.csv'), quote = FALSE)
    write.csv(t(X),fileName('wide.csv'), quote = FALSE)

    #generate responses
    weights <- rnorm(nVariables)
    names(weights) <- varNames
    y <- X %*% weights
    y_cont <- y[,1] / maxValue
    y_bi <-as.numeric(y_cont>=0)
    y_cat5 <- as.numeric(cut(y_cont, 5)) -1
    y_cat10 <- as.numeric(cut(y_cont, 10)) -1
    write.csv(data.frame(cont=y_cont, cat2=y_bi, cat5 = y_cat5, cat10 = y_cat10), fileName('labels.csv'), quote = FALSE)
})



