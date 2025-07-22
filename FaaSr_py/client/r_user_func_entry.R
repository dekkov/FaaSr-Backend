install.packages(c("jsonlite", "httr"), repos = "https://cloud.r-project.org")
library("jsonlite")
library("httr")
source("http_wrappers.R")
source("r_func_helper.R")

args <- commandArgs(trailingOnly = TRUE)
func_name = args[1]
user_args = fromJSON(args[2])
invocationID = args[3]

faasr_source_r_files(paste0("/tmp/functions", invocationID))

# Execute User function
faasr_run_user_function(func_name, user_args)

faasr_return(TRUE)


