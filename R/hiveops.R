sparkops.createhivetable <- function(schema,table_name,hdfs_location){
tryCatch({
  print('Creating hive table based on schema')
  begin_hive_statment <- paste('create external table',table_name)
  character_columns <- schema.charactercolumns(schema)
  character_ddl <- paste(character_columns,'string', sep = ' ')
  integer_columns <- schema.integercolumns(schema)
  integer_ddl <- paste(integer_columns,'integer',sep = ' ')
  numeric_columns <- schema.numericcolumns(schema)
  numeric_ddl <- paste(numeric_columns,'double',sep = ' ')

  body_ddl <- NULL
  if(length(character_columns)!= 0){
    body_ddl <- character_ddl
  }
  if(length(integer_columns) != 0){
    if(!is.null(body_ddl) | length(body_ddl) != 0){
      body_ddl <- c(body_ddl,integer_ddl)
    }else{
      body_ddl <- integer_ddl
    }
  }
  if(length(numeric_columns) != 0){
    if(!is.null(body_ddl) | length(body_ddl) != 0){
      body_ddl <- c(body_ddl,numeric_ddl)
    }else{
      body_ddl <- numeric_ddl
    }
  }
  body_ddl <-  paste(body_ddl,collapse = ',')

  ending_ddl <- paste("STORED AS PARQUET LOCATION","'",paste(gethdfsroot(),hdfs_location,sep = '/'),"'")

  table_definition <- paste(begin_hive_statment,'(',body_ddl,")",ending_ddl,sep = '\n')

  }, warning = function(w) {
      print(paste('WARN: ',w))

  }, error = function(e) {
      print(paste('ERROR: ',e))


  }, finally = {

  })}
