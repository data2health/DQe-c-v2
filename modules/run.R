if(!require("optparse")) install.packages("optparse", repos = "http://cran.us.r-project.org")
if(!require("reticulate")) install.packages("reticulate", repos = "http://cran.us.r-project.org")
if (!require("data.table")) install.packages("data.table"), repos = "http://cran.us.r-project.org")
if (!require("dplyr")) install.packages("dplyr", repos = "http://cran.us.r-project.org")
if (!require("ggplot2")) install.packages("ggplot2", repos = "http://cran.us.r-project.org")
if (!require("gridExtra")) install.packages("gridExtra", repos = "http://cran.us.r-project.org")
if (!require("rmarkdown")) install.packages("rmarkdown", repos = "http://cran.us.r-project.org")
if (!require("knitr")) install.packages("knitr", repos = "http://cran.us.r-project.org")
if (!require("plotly")) install.packages("plotly", repos = "http://cran.us.r-project.org")
if (!require("DT")) install.packages("DT", repos = "http://cran.us.r-project.org")
if (!require("treemap")) install.packages("treemap", repos = "http://cran.us.r-project.org")
if (!require("visNetwork")) install.packages("visNetwork", repos = "http://cran.us.r-project.org")


library("optparse")
 
option_list = list(
  make_option(c("-c", "--config"), type="character", default=NULL, 
              help="configuration file for DQe-c", metavar="character"),
	make_option(c("-o", "--out"), type="character", default=NULL, 
              help="output file name", metavar="character")
); 
 
opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);

rmarkdown::render("modules/report.Rmd", output_file="report.html", output_dir=paste(".", opt$out, sep=""), params=list(
    config = opt$config,
    outputDir = paste("..", opt$out, sep="")
))