library(httr)

faasr_log <- function(log_message) {
    request_json <- list(
        "ProcedureID" = "faasr_log",
        Arguments = list(
            "log_message" = log_message
        )
    )
    r <- POST("http://127.0.0.1:8000/faasr-action", body=request_json, encode="json")
    response_content <- content(r)

    if (!is.null(response_content$Success) && response_content$Success) {
        return (response_content$Data$folder_list)
    } else {
        err_msg <- paste0('{\"faasr_log\": \"Request to FaaSr RPC failed\"}')
        message(err_msg)
        faasr_exit(error=TRUE)
    }
}


faasr_put_file <- function(local_file, remote_file, server_name="", local_folder=".", remote_folder=".") {
    print("put file wrapper!")
    request_json <- list(
        "ProcedureID" = "faasr_put_file",
        "Arguments" = list("local_file" = local_file, 
                    "remote_file" = remote_file,
                    "server_name" = server_name,
                    "local_folder" = local_folder,
                    "remote_folder" = remote_folder
        )
    )
    r <- POST("http://127.0.0.1:8000/faasr-action", body=request_json, encode="json")
    response_content <- content(r)

    if (!is.null(response_content$Success) && response_content$Success) {
        return (response_content$Data$folder_list)
    } else {
        err_msg <- paste0('{\"faasr_put_file\": \"request to FaaSr RPC failed\"}')
        message(err_msg)
        faasr_exit(error=TRUE)
    }
}
    

faasr_get_file <- function(local_file, remote_file, server_name="", local_folder=".", remote_folder=".") {
    request_json <- list(
        "ProcedureID" = "faasr_get_file",
        "Arguments" = list ("local_file" = local_file, 
                    "remote_file" = remote_file,
                    "server_name" = server_name,
                    "local_folder" = local_folder,
                    "remote_folder" = remote_folder
        )
    )
    r <- POST("http://127.0.0.1:8000/faasr-action", body=request_json, encode="json")
    response_content <- content(r)

    if (!is.null(response_content$Success) && response_content$Success) {
        return (response_content$Data$folder_list)
    } else {
        err_msg <- paste0('{\"faasr_get_file\": \"Request to FaaSr RPC failed\"}')
        message(err_msg)
        faasr_exit(error=TRUE)
    }
}


faasr_delete_file <- function(remote_file, server_name="", remote_folder="") {
    request_json <- list(
        "ProcedureID" = "faasr_delete_file",
        "Arguments" = list("remote_file" = remote_file, 
                    "server_name" = server_name,
                    "remote_folder" = remote_folder
        )
    )
    r <- POST("http://127.0.0.1:8000/faasr-action", body=request_json, encode="json")
    response_content <- content(r)

    if (!is.null(response_content$Success) && response_content$Success) {
        return (response_content$Data$folder_list)
    } else {
        err_msg <- paste0('{\"faasr_delete_file\": \"Request to FaaSr RPC failed\"}')
        message(err_msg)
        faasr_exit(error=TRUE)
    }
}


faasr_get_folder_list <- function(server_name="", faasr_prefix = "") {
    request_json <- list(
        "ProcedureID" = "faasr_get_folder_list",
        "Arguments" = list("server_name" = server_name,
                     "faasr_prefix" = faasr_prefix
                     )
    )
    r <- POST("http://127.0.0.1:8000/faasr-action", body=request_json, encode="json")
    response_content <- content(r)
    
    if (!is.null(response_content$Success) && response_content$Success) {
        return (response_content$Data$folder_list)
    } else {
        err_msg <- paste0('{\"faasr_get_folder_list\": \"failed to get folder list\"}')
        message(err_msg)
        faasr_exit(error=TRUE)
    }
}


faasr_return <- function(return_value=NULL) {
    return_json <- list(
        FunctionResult = return_value
    )
    r <- POST("http://127.0.0.1:8000/faasr-return", body=return_json, encode="json")
    quit()
}


faasr_exit <- function(message=NULL, error=TRUE) {
    exit_json <- list(
        Error = error,
        Message = message
    )
    r <- POST("http://127.0.0.1:8000/faasr-exit", body=exit_json, encode="json")
    quit()
}