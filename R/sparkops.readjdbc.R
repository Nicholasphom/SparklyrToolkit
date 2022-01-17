sparkops.readjdbc <- function(sc,name,database_url,database_name,database_tablename,driver_classname,database_user,database_password,repartition = 0){
tryCatch({
  status <- FALSE
  sc <- nick_spark_connect()
  if(!base::is.null(sc)){
  db_tbl <- spark_read_jdbc(sc,
                            name    = name,
                            options = list(url      = paste(database_url,database_name,sep =''),
                                           user     = database_user,
                                           password = database_password,
                                           dbtable  = database_tablename,
                                           driver = driver_classname),repartition = repartition)
  }
  if(sdf_nrow(db_tbl) == 0){
    print('DATABASE READ READ 0 or NULL ROWS')
  }else{
    status <- TRUE
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
