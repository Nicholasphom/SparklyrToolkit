sparkops.writejdbctable <- function(x,name,database_url,database_name,driver_classname,database_user,database_password,mode = 'overwrite'){
  tryCatch({
    status <- FALSE
    sc <- nick_spark_connect()
    if(!base::is.null(sc)){
      db_tbl <- spark_write_jdbc(x = x,
                                name    = name,
                                options = list(url      = paste(database_url,database_name,sep =''),
                                               user     = database_user,
                                               password = database_password,
                                               driver = driver_classname),mode = mode)
    }
    if(db_tbl){
      print('WRITE SUCCESS')
      status <- TRUE
    }else{
      status <- FALSE
    }


  }, warning = function(w) {
    print(paste('WARN: ',w))

  }, error = function(e) {
    print(paste('ERROR: ',e))


  }, finally = {
    if(status){
      print('FUNCTION SUCCESSS')
    }else{
      print('FUNCTION FAILED')
    }
    return(db_tbl)
  })}
