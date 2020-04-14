
### Install and load all libraries 
if (!require("easypackages")) install.packages('easypackages', repos = "http://cran.us.r-project.org")
if (!require("data.table")) install.packages("data.table", repos = "http://cran.us.r-project.org")
if (!require("devtools")) install.packages("devtools", repos = "http://cran.us.r-project.org")
if (!require("dplyr")) install.packages("dplyr", repos = "http://cran.us.r-project.org")
if (!require("DT")) install.packages("DT", repos = "http://cran.us.r-project.org")
if (!require("ggplot2")) install.packages("ggplot2", repos = "http://cran.us.r-project.org")
if (!require("gridExtra")) install.packages("gridExtra", repos = "http://cran.us.r-project.org")
if (!require("optparse")) install.packages("optparse", repos = "http://cran.us.r-project.org")
if (!require("knitr")) install.packages("knitr", repos = "http://cran.us.r-project.org")
if (!require("rmarkdown")) install.packages("rmarkdown", repos = "http://cran.us.r-project.org")
if (!require("plotly")) install.packages("plotly", repos = "http://cran.us.r-project.org")
if (!require("treemap")) install.packages("treemap", repos = "http://cran.us.r-project.org")
if (!require("reshape2")) install.packages("reshape2", repos = "http://cran.us.r-project.org")
if (!require("visNetwork")) install.packages("visNetwork", repos = "http://cran.us.r-project.org")
if (!require("rmdformats")) install.packages("rmdformats", repos = "http://cran.us.r-project.org")
if (!require("ggrepel")) install.packages("ggrepel", repos = "http://cran.us.r-project.org")
if (!require("flexdashboard")) install.packages("flexdashboard", repos = "http://cran.us.r-project.org")
if(!require("reticulate")) install.packages("reticulate", repos = "http://cran.us.r-project.org")


library("optparse")
 
option_list = list(
	make_option(c("-o", "--out"), type="character", default=NULL, 
              help="output file name", metavar="character")
); 
 
opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser);


## then generate the html report
rmarkdown::render("modules/network_view.Rmd", output_file="network_report.html", output_dir=opt$out)





