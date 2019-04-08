library(randomForest)

X <- read.csv(bzfile('../../../data/har_aal.csv.bz2'), header = TRUE, row.names = 1)
y <- read.csv('../../../data/har_aal_labels.csv', row.names=1)

