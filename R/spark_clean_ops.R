
sparkcleanops.imputeintegercols <- function(data,columns,name,method = 'mean'){
tryCatch({
  imputed_data_set <- data
  for(i in 1:length(columns)){
    column_name_sym <-  rlang::sym(columns[i])
    if(method == 'mean'){
      imputed_data_set <- imputed_data_set %>%
        dplyr::mutate({
          {
            column_name_sym
          }
        } := dplyr::if_else(is.na({
          {
            column_name_sym
          }
        }), as.integer(round(mean(column_name_sym))), {
          {
            column_name_sym
          }
        }))

    }else if(method == 'median'){
      imputed_data_set <- imputed_data_set %>%
        dplyr::mutate({
          {
            column_name_sym
          }
        } := dplyr::if_else(is.na({
          {
            column_name_sym
          }
        }), median(column_name_sym), {
          {
            column_name_sym
          }
        }))

    }

    if(i %% 10 == 0){
      new_imputed_data <- imputed_data_set
      sparklyr::spark_write_table(new_imputed_data,name = name,mode = 'overwrite')
      print('Writing Imputed Results to Table to prevent iterations errors')
    }
    print(paste('IMPUTED INTEGER COLUMN ', column_name_sym))
  }

  }, warning = function(w) {
      print(paste('WARN: ',w))

  }, error = function(e) {
      print(paste('ERROR: ',e))


  }, finally = {
    sparklyr::spark_write_table(imputed_data_set,name = name,mode = 'overwrite')
    return(imputed_data_set)


  })}


sparkcleanops.imputecharactercols <- function(data,columns,name){
  tryCatch({
    imputed_data_set <- data
    impute_value <- 'NOT AVAILABLE'
    for(i in 1:length(columns)){
      column_name_sym <-  rlang::sym(columns[i])
      imputed_data_set <- imputed_data_set %>%
        dplyr::mutate({
          {
            column_name_sym
          }
        } := dplyr::if_else(is.na({
          {
            column_name_sym
          }
        }), impute_value, {
          {
            column_name_sym
          }
        }))
      if(i %% 10 == 0){
        new_imputed_data <- imputed_data_set
        sparklyr::spark_write_table(new_imputed_data,name = name,mode = 'overwrite')
        print('Writing Imputed Results to Table to prevent iterations errors')
      }
      print(paste('IMPUTED COLUMN ', column_name_sym))
    }

  }, warning = function(w) {
    print(paste('WARN: ',w))

  }, error = function(e) {
    print(paste('ERROR: ',e))


  }, finally = {
    sparklyr::spark_write_table(imputed_data_set,name = name,mode = 'overwrite')
    return(imputed_data_set)


  })}



sparkcleanops.imputenumericcols <- function(data,columns,name,method = 'mean'){
  tryCatch({
    imputed_data_set <- data
    for(i in 1:length(columns)){
      column_name_sym <-  rlang::sym(columns[i])
      if(method == 'mean'){
        imputed_data_set <- imputed_data_set %>%
          dplyr::mutate({
            {
              column_name_sym
            }
          } := dplyr::if_else(is.na({
            {
              column_name_sym
            }
          }), as.numeric(round(mean(column_name_sym))), {
            {
              column_name_sym
            }
          }))

      }else if(method == 'median'){
        imputed_data_set <- imputed_data_set %>%
          dplyr::mutate({
            {
              column_name_sym
            }
          } := dplyr::if_else(is.na({
            {
              column_name_sym
            }
          }), as.numeric(round(median(column_name_sym))), {
            {
              column_name_sym
            }
          }))

      }

      if(i %% 10 == 0){
        new_imputed_data <- imputed_data_set
        sparklyr::spark_write_table(new_imputed_data,name = name,mode = 'overwrite')
        print('Writing Imputed Results to Table to prevent iterations errors')
      }
      print(paste('IMPUTED NUMERIC COLUMN ', column_name_sym))
    }

  }, warning = function(w) {
    print(paste('WARN: ',w))

  }, error = function(e) {
    print(paste('ERROR: ',e))


  }, finally = {
    sparklyr::spark_write_table(imputed_data_set,name = name,mode = 'overwrite')
    return(imputed_data_set)


  })}
