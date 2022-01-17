schema.numericcolumns <- function(schema){
tryCatch({
  return(schema %>% dplyr::filter(Column_Types == 'numeric') %>% pull(Column_Names))

  }, warning = function(w) {
      print(paste('WARN: ',w))

  }, error = function(e) {
      print(paste('ERROR: ',e))


  }, finally = {

  })}


schema.charactercolumns <- function(schema){
  tryCatch({
    return(schema %>% dplyr::filter(Column_Types == 'character') %>% pull(Column_Names))

  }, warning = function(w) {
    print(paste('WARN: ',w))

  }, error = function(e) {
    print(paste('ERROR: ',e))


  }, finally = {

  })}


schema.integercolumns <- function(schema){
  tryCatch({
    return(schema %>% dplyr::filter(Column_Types == 'integer') %>% pull(Column_Names))

  }, warning = function(w) {
    print(paste('WARN: ',w))

  }, error = function(e) {
    print(paste('ERROR: ',e))


  }, finally = {

  })}
