sparkops.readparquet <- function(path,name){
tryCatch({
  status <- FALSE
  sc <- nick_spark_connect()
  if(!base::is.null(sc)){
    spark_read <- sparklyr::spark_read_parquet(sc,path = base::paste(gethdfsroot(),path,sep = '/'),name = name)
    if(!base::is.null(spark_read)){
      status <- TRUE
    }

  }

  }, warning = function(w) {
    print(paste('WARN:',w,sep = ' '))

  }, error = function(e) {
      print(paste('ERROR:',e,sep = ' '))


  }, finally = {
    if(status){
      print('Spark Read Parquet Successfull')
    }else{
      print('Spark Read Not Successfull')
    }
    return(spark_read)

  })}




sparkops.readcsv <- function(path,name){
  tryCatch({
    status <- FALSE
    sc <- nick_spark_connect()
    if(!base::is.null(sc)){
      spark_read <- sparklyr::spark_read_csv(sc,path = path,name = name)
      if(!base::is.null(spark_read)){
        status <- TRUE
      }

    }

  }, warning = function(w) {
    print(paste('WARN:',w,sep = ' '))

  }, error = function(e) {
    print(paste('ERROR:',e,sep = ' '))

  }, finally = {
    if(status){
      print('Spark Read CSV Successfull')
    }else{
      print('Spark Read CSV Successfull')
    }
    return(spark_read)

  })}


sparkops.readtable <- function(table_name){
tryCatch({
  spark_df <- NULL
  status <- FALSE
  sc <- nick_spark_connect()
  spark_df <- sparklyr::spark_read_table(sc,name = table_name)
  if(!base::is.null(spark_df) & sdf_nrow(spark_df) != 0){
    status <- TRUE
  }
}, warning = function(w) {
  print(paste('WARN:',w,sep = ' '))

}, error = function(e) {
  print(paste('ERROR:',e,sep = ' '))


  }, finally = {
    if(status){
      print('SPARK READ TABLE SUCCESSFULL')

    }else{
      print('SPARK READ TABLE FAILED')
    }
    return(spark_df)
  })}

