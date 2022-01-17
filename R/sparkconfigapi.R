getsparkhome <- function(){
  return(spark_home)
}

getsparkmaster <- function() {
  return(spark_master)
}

gethdfsroot <- function(){
  return(hdfs_root)
}

get_spark_app_name <- function(){
  return(spark_app_name)
}

getsparkclass <- function(){
  return(spark_df_class)
}
getrclass <- function(){
  return(r_df_class)
}

nick_spark_connect <- function(){
  conf <- spark_config()   # Load variable with spark_config()

  conf$spark.executor.memory <- "12G"
  conf$spark.memory.fraction <- 0.9
  conf$`sparklyr.shell.driver-memory` <- "3G"
  conf$`sparklyr.shell.driver-class-path` <- 'Users/nickphom/development/jars/mysql-connector-java-8.0.27.jar'
  sc <- sparklyr::spark_connect(master = getsparkmaster(),spark_home = getsparkhome(),app_name = get_spark_app_name(),config = conf)
  return(sc)
}


getmysqlserver <- function(){
  return(mysql_url)
}

getmysqluser <- function(){
  return(mysql_user)
}


getmysqlpassword <- function(){
  return(mysql_password)
}


getmysqldriver <- function(){
  return(mysql_driver_class)
}
