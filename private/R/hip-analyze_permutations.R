library(ggplot2)

CHR_LENGHTS_BP <-c(
249250621,243199373,198022430,191154276,180915260,171115067,159138663,146364022,141213431,135534747,135006516,133851895,115169878,107349540,102531392,90354753,81195210,78077248,59128983,63025520,48129895,51304566
)


CHR_START_BP = cumsum(c(0, CHR_LENGHTS_BP))

variable_to_coord <-function(var_name) {
    elements <- strsplit(var_name,'_')[[1]]
    list(chr=elements[1], pos=as.numeric(elements[2]))
}

variable_to_pos <-function(var_name) {
    coord <- variable_to_coord(var_name)
    CHR_START_BP[as.numeric(coord$chr)] + coord$pos
}

null_df <- read.csv(Sys.glob('./null_imp_hipster_2000_100.csv/part-*.csv'), row.names = 'variable')

mean_sd_df_all <- data.frame(variable=row.names(null_df), 
                         mean=apply(null_df, MARGIN = 1, mean),
                         sd=apply(null_df, MARGIN = 1, sd),
                         nz_count=apply(null_df, MARGIN = 1, function(x){sum(x>0)})
)

min_sd <- 0.01
min_nz_count <- 10
mean_sd_df <- 
    mean_sd_df_all[mean_sd_df_all$sd > min_sd & mean_sd_df_all$nz_count > min_nz_count,]


imp_df <- read.csv('./imp_hipster.csv', stringsAsFactors = FALSE)

merged_df <- merge(imp_df, mean_sd_df)

p_values <-pnorm((merged_df$importance-merged_df$mean)/merged_df$sd,lower.tail=FALSE)

p_values_df <-data.frame(p_value = p_values,
                         p_score = -log10(sapply(p_values, FUN=max, 10e-12)),
                         row.names = merged_df$variable, importance = merged_df$importance, 
                         pos=sapply(merged_df$variable, FUN=variable_to_pos),
                         chr=sapply(merged_df$variable, FUN=function(n){variable_to_coord(n)$chr}),
                         null_mean = merged_df$mean,
                         null_sd = merged_df$sd
)

print(
ggplot(p_values_df[order(p_values_df$pos),], aes(x=seq_along(p_values_df$pos), y=importance, color=chr)) 
    + geom_line(aes(), alpha=0.3) 
    + geom_point(aes(y=p_score), alpha=0.3)
    + theme_bw() 
    + scale_y_continuous("Gini Importance", sec.axis = dup_axis(name="-log10(p-value)"))
    + scale_x_continuous("Variant Index")
)

