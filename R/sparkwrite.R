sparkops.writeparquet <- function(tbl,path,mode = 'overwrite'){
  tryCatch({
    status <- FALSE
    sc <- nick_spark_connect()
    if(!base::is.null(sc)){
      if(!base::is.null(tbl)){
        status <- TRUE
        spark_write <- tbl %>%  sparklyr::sdf_coalesce(10)

        sparklyr::spark_write_parquet(spark_write,path = paste(gethdfsroot(),path,sep = '/'),mode = mode)
      }

    }

  }, warning = function(w) {
    print(paste('WARN:',w,sep = ' '))

  }, error = function(e) {
    print(paste('ERROR:',e,sep = ' '))

  }, finally = {
    if(status){
      print('Spark WRITE  Successfull')
    }else{
      print('Spark WRITE  UNSuccessfull')
    }
    return(spark_write)

  })}
