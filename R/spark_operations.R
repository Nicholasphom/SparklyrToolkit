sparkops.droptable <- function(table_name){
tryCatch({
  status <- FALSE
  sc <- nick_spark_connect()
  if(!base::is.null(sc)){
    sdf_sql(sc,base::paste("Drop Table",table_name,sep = ' '))
    status <- TRUE

  }else{
    print('CANNOT DROP TABLE')
  }

  }, warning = function(w) {
      message(sprintf("Warning in %s: %s", deparse(w[["call"]]), w[["message"]]))

  }, error = function(e) {
      message(sprintf("Error in %s: %s", deparse(e[["call"]]), e[["message"]]))


  }, finally = {
    if(status){
      print('Dropped Table')
    }else{
      print('Could not drop table!')
    }


  })}


sparkops.getschema <- function(data){
tryCatch({
  schema_df <- NULL
  status <- FALSE
  if(identical(class(data),getsparkclass())){
    print('Spark_dataframe Detected,converting to R Dataframe')
    schema_df <- data %>% head(1) %>% dplyr::collect()
  }else if(identical(class(data),getrclass())){
    print('R Dataframe Detected, getting schema')
    schema_df <- data
  }else{
    print('Unknown Dataframe Type')
    return('ERROR')
  }
  class_list <- sapply(colnames(schema_df), class)
  val_list <- colnames(schema_df)
  full_schema_df <- data.frame('Column_Names' = val_list,'Column_Types' = class_list,stringsAsFactors = FALSE)
  row.names(full_schema_df) <- NULL
  if(nrow(schema_df) != 0){
    status <- TRUE
  }


  }, warning = function(w) {
      message(sprintf("Warning in %s: %s", deparse(w[["call"]]), w[["message"]]))

  }, error = function(e) {
      message(sprintf("Error in %s: %s", deparse(e[["call"]]), e[["message"]]))


  }, finally = {
    if(status){
      print('Function Success')
      return(full_schema_df)
    }else{
      print('Function Fail')
      return('ERROR')
    }


  })}


sparkops.dropalltables <- function(){
tryCatch({
  sc <- nick_spark_connect()
  table_lists <- dplyr::src_tbls(sc)
  for(i in 1:length(table_lists)){
    sdf_sql(sc,base::paste("Drop Table",table_lists[i],sep = ' '))
    print(paste('Dropped TABLE',table_lists[i], sep = ' '))
  }

  }, warning = function(w) {
      print(paste('WARN: ',w))

  }, error = function(e) {
      print(paste('ERROR: ',e))


  }, finally = {


  })}
