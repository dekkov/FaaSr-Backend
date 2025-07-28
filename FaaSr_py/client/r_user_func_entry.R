library("jsonlite")
library("httr")
source("http_wrappers.R")
source("r_func_helper.R")

args <- commandArgs(trailingOnly = TRUE)
func_name = args[1]
user_args = fromJSON(args[2])
invocation_id = args[3]

faasr_source_r_files(file.path("/tmp/functions", invocation_id))

# Execute User function
result <- faasr_run_user_function(func_name, user_args)

faasr_return(result)


