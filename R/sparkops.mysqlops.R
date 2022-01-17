sparkops.readmysqldb <- function(mysql_tablename,mysql_database_name,name,repartition = 0){
tryCatch({
    sc <- nick_spark_connect()
    if(!is.null(sc)){
    mysql_read <- sparkops.readjdbc(sc = sc,database_url = getmysqlserver(),database_tablename = mysql_tablename
                                    ,driver_classname = getmysqldriver(),database_name = mysql_database_name,database_user = getmysqluser()
                                    ,database_password = getmysqlpassword(),name = name,repartition = repartition)
    }
  }, warning = function(w) {
      print(paste('WARN: ',w))

  }, error = function(e) {
      print(paste('ERROR: ',e))


  }, finally = {
    return(mysql_read)
  })}


sparkops.writwemysqltable <- function(x,name,database_name, mode = 'overwrite'){
  tryCatch({
    sc <- nick_spark_connect()
    if(!is.null(sc)){
      mysql_read <- sparkops.writejdbctable(x = x,name = name
                                            ,database_url = getmysqlserver(),database_name = database_name
                                            ,database_user = getmysqluser(),database_password = getmysqlpassword()
                                            ,driver_classname = getmysqldriver(),mode = mode)
    }
  }, warning = function(w) {
    print(paste('WARN: ',w))

  }, error = function(e) {
    print(paste('ERROR: ',e))


  }, finally = {
    return(mysql_read)
  })}



