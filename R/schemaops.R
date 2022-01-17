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



sparkops.applyschema <- function(data,schema){
tryCatch({
  changed_data <- NULL

  integer_cols <- schema.integercolumns(schema)
  numeric_cols <- schema.numericcolumns(schema)
  character_col <- schema.charactercolumns(schema)
  if(length(integer_cols) == 0){
    integer_cols <- NULL
  }
  if(length(character_col) == 0){
    character_col <- NULL
  }
  if(length(numeric_cols) == 0){
    numeric_cols <- NULL
  }

  changed_data <- data %>%
    mutate(across( integer_cols, as.integer)) %>%
    mutate(across( numeric_cols, as.numeric)) %>%
    mutate(across( character_col, as.character))
  }, warning = function(w) {
      print(paste('WARN: ',w))

  }, error = function(e) {
      print(paste('ERROR: ',e))


  }, finally = {
    return(changed_data)

  })}
