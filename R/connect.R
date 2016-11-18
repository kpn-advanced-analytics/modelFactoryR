# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Build and Reload Package:  'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'

#TODO(chris): Add function to get last session_id jusr befor table changed based on Informatica update log table
#TODO(chris): Add function for getting out of sample data based on old session_id
#TODO(chris): Add function to get git changes since session_id date


#' Get connection
#'
#' @examples
#' getTaConnection()
#' @return The Aster connection object
#' @export
getTaConnection <- function() {
  # Test if the current system has kpnr config
  if (file.exists("~/.kpnr/config.yaml")) {
    config <- yaml::yaml.load_file("~/.kpnr/config.yaml")
    options(kpnConfig = config)
  } else if (Sys.getenv("USER") == "jenkins"){
    config <- yaml::yaml.load_file("/etc/jenkins/config.yaml")
    options(kpnConfig = config)
  }else{
    # Assume that we are running in the streaming API on Aster
    file.in <- file(description = "stdin", open = "r")
    input <-
      read.table(
        file.in,
        header = FALSE,
        sep = "\t",
        quote = "",
        na.strings = c("", "NA"),
        comment.char = ""
      )
    names(input) <- c('user_aster',
                      'password_aster',
                      'user_teradata',
                      'password_teradata')
    config <<- list()
    config$aster$username <<- levels(input$user_aster[0])
    config$aster$password <<- levels(input$password_aster[0])
    config$aster$odbc_dsn <<- "AsterDSN"
    config$teradata$username <<- levels(input$user_teradata[0])
    config$teradata$password <<- levels(input$password_teradata[0])
    options(kpnConfig = config)
    options(AsterStreaming = TRUE)
  }
  TeradataAsterR::ta.connect(dsn=config$aster$odbc_dsn,uid = config$aster$username, pwd = config$aster$password)
}

#' Get a new random session_id and store the session in Aster model_factory.run_history table
#'
#' @param model_id The current model ID
#' @examples
#' model_id <- c('titanic_training')
#' getSessionId(model_id)
#' @return A new session ID
#' @export
getSessionId <- function(model_id) {
  config <- getOption("kpnConfig")
  check_model_id <-
    taQuery(sprintf(
      "select * from model_factory.model_overview where model_id='%s'",
      model_id
    ))

  if (nrow(check_model_id) == 0)
  {
    stop(
      simpleError(
        "Given model_id is not present in model_factory.model_overview table. Please use function addModelId first"
      )
    )
  }
  else {
    session_id <<-
      paste(
        config$aster$username,
        model_id,
        format(Sys.time(), "%Y%m%d"),
        floor(runif(1, 1000, 1000000)),
        sep = '_'
      )
    model_id <<- model_id
    taQuery(
      sprintf(
        "INSERT INTO model_factory.run_history (session_id,  user_id, model_id, start_time) VALUES ('%s', '%s', '%s', '%s')",
        session_id,
        config$aster$username,
        model_id,
        as.character(Sys.time())
      )
    )
  }
}


#' Add model_id and model overview in model_factory.model_overview table
#'
#' @param model_id The current model ID
#' @param model_description Model description
#' @param score_id_type Type of score_id (loc_id,CKR, account etc)
#' @examples
#' addModelId('titanic_training','Training on Titanic data','passengerid')
#' @return The result of taQuery to update the model_factory.model_overview table
#' @export
addModelId <- function(model_id,
                       model_description,
                       score_id_type) {
  config <- getOption("kpnConfig")
  check_model_id <-
    taQuery(sprintf(
      "select * from model_factory.model_overview where model_id='%s'",
      model_id
    ))

  if (nrow(check_model_id) > 0)
  {
    stop(
      simpleError(
        "Given model_id is already present in model_factory.model_overview table"
      )
    )
  }
  else {
    taQuery(
      sprintf(
        "INSERT INTO model_factory.model_overview (model_id,model_description,score_id_type) VALUES ('%s', '%s', '%s')",
        model_id,
        model_description,
        score_id_type
      )
    )
  }
}


#' Remove model_id and model overview from model_factory.model_overview table
#'
#' @param model_id The vector of model id's
#' @examples
#' deleteModelId(c('titanic_training'))
#' @return The result of taQuery to update the model_factory.model_overview table
#' @export
deleteModelId <- function(model_id) {
  config <- getOption("kpnConfig")
  check_model_id <-
    taQuery(sprintf(
      "select * from model_factory.model_overview where model_id in ('%s')",
      paste(model_id,collapse='\',\'')
    ))

  if (nrow(check_model_id) == 0)
  {
    stop(
      simpleError(
        "Given model_id is already removed from model_factory.model_overview table"
      )
    )
  }
  else {
    taQuery(
      sprintf(
        "delete from model_factory.model_overview where model_id in ('%s')",
        paste(model_id,collapse='\',\'')
      )
    )
  }
}

#' Update threshold information in model_factory.model_overview table
#'
#' @param model_id The current model ID
#' @param threshold_value Chosen threshold_value
#' @param threshold_type Threshold type ("population" or "probability")
#' @examples
#' updateThreshold('titanic_training','0.5','probability')
#' @return The result of taQuery to update the model_factory.model_overview table
#' @export
updateThreshold <- function(model_id, threshold_value, threshold_type) {
  config <- getOption("kpnConfig")
  check_model_id <-
    taQuery(sprintf(
      "select * from model_factory.model_overview where model_id='%s'",
      model_id
    ))

  if (nrow(check_model_id) == 0)
  {
    stop(simpleError(
      "Given model_id is not present in model_factory.model_overview table"
    ))
  }
  else {
    if (threshold_type != "probability" &
        threshold_type != "population")
    {
      stop(simpleError("Given threshold type is not population or probability"))
    }
    else {
      taQuery(
        sprintf(
          "Update model_factory.model_overview set threshold_value = '%s', threshold_type='%s' where model_id='%s'",
          threshold_value,
          threshold_type,
          model_id
        )
      )
    }
  }
}



#' Close the current session and update the end_time of run in model_factory.run_history table for current session session with the current timestamp
#'
#' @examples
#' closeSession()
#' @return The result of taQuery to update the model_factory.run_history table
#' @export
closeSession <- function() {
  config <- getOption("kpnConfig")
  taQuery(
    sprintf(
      "UPDATE model_factory.run_history  SET end_time = '%s' where session_id='%s'",
      as.character(Sys.time()),
      session_id
    )
  )
  x <- data.frame('session_id' = session_id)
  write.table(
    x, stdout(), col.names = FALSE,
    row.names = TRUE, quote = FALSE, sep = '\t'
  )
}

#' Remove all data associated with a session
#'
#' @param session_id The vector of session id's
#' @examples
#' deleteSession(c('cvpm_t9_vecht499_20160627_949068'))
#' @return True if successful
#' @export
deleteSession <- function(session_id) {
  config <- getOption("kpnConfig")
  try(taQuery(
    sprintf(
      "DELETE FROM model_factory.model_summary WHERE session_id in ('%s')",
      paste(session_id,collapse='\',\'')
    )
  ),silent=TRUE)
  try(taQuery(
    sprintf(
      "DELETE FROM model_factory.model_scores WHERE session_id in ('%s')",
      paste(session_id,collapse='\',\'')
    )
  ),silent=TRUE)
  try(taQuery(
    sprintf(
      "DELETE FROM model_factory.model_test_results WHERE session_id in ('%s')",
      paste(session_id,collapse='\',\'')
    )
  ),silent=TRUE)
  try(taQuery(
    sprintf(
      "DELETE FROM model_factory.model_backtesting WHERE session_id in ('%s')",
      paste(session_id,collapse='\',\'')
    )
  ),silent=TRUE)
  try(taQuery(
    sprintf(
      "DELETE FROM model_factory.metadata_table WHERE session_id in ('%s')",
      paste(session_id,collapse='\',\'')
    )
  ),silent=TRUE)
  try(taQuery(sprintf(
    "DELETE FROM model_factory.run_history WHERE session_id in ('%s')",
    paste(session_id,collapse='\',\'')
  )),silent=TRUE)
  return(TRUE)
}


#' Creates a data.frame with the mean,sd,median,st,max,min, and number of NAs in a given dataframe or ta.data.frames
#'
#' @param dataframe to summerize
#' @examples
#' train_tadf <- ta.data.frame("select * from model_factory.titanic_train",sourceType="query")
#' getSummary(train_tadf)
#'
#' train_df <- ta.pull(train_tadf)
#' getSummary(train_df)
#' @return The summary
#' @export
getSummary <- function(dataset) {
  if (class(dataset) == "data.frame"){
    my.summary <- function(x, na.rm = TRUE) {
      result <- c(
        mean = mean(x, na.rm = na.rm),
        sd = sd(x, na.rm = na.rm),
        median = median(x, na.rm = na.rm),
        min = min(x, na.rm = na.rm),
        max = max(x, na.rm = na.rm),
        n = length(x),
        n_na = sum(is.na(x))
      )
    }

    ind <- sapply(dataset, is.numeric)
    y <- data.frame(sapply(dataset[, ind], my.summary))
    z <- data.frame(t(y))
    z$variable <- rownames(z)
    summary <- data.frame(z, row.names = NULL)
    return(summary)
  }
  else  if (class(dataset) == "ta.data.frame"){
    my.summary <- function(x, na.rm = TRUE) {
      x1 <- as.numeric(x)
      result <- c(
        mean = mean(x1, na.rm = na.rm),
        sd = sd(x1, na.rm = na.rm),
        median = median(x1, na.rm = na.rm),
        min = min(x1, na.rm = na.rm),
        max = max(x1, na.rm = na.rm),
        n = length(x),
        n_na = sum(is.na(x))
      )
    }

    ind <- ta.coltypes(dataset) %in% c("int","smallint","numeric","bigint","double")
    y <- ta.colApply(dataset[,names(dataset)[ind]],FUN=my.summary)
    z <- data.frame(t(y))
    z$variable <- rownames(z)
    summary <- data.frame(z, row.names = NULL)
    return(summary)
  }
  else
  {
    stop(
      simpleError(
        "Given dataset does not have class ta.data.frame or data.frame. Please give a ta.data.frame or data.frame as input"
      )
    )
  }
}

#' Write summary of the data frame to the summary table (model_factory.model_summary)
#'
#' @param dataset Dataframe which is the result of getSummary to be added unto the summary table
#' @examples
#' model_id <- c('titanic_training')
#' getSessionId(model_id)
#' train_tadf <- ta.data.frame("select * from model_factory.titanic_train",sourceType="query")
#' train <- ta.pull(train_tadf)
#' summary <- getSummary(train)
#' storeSummary(summary)
#' @return The result of taQuery to update the model_factory.model_summary table
#' @export
storeSummary <- function(dataset) {
  if (class(dataset) != "data.frame")
  {
    stop(
      simpleError(
        "Given dataset does not have class data.frame. Please give data.frame as input"
      )
    )
  }
  else {
    select_query <- sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(paste(session_id, "summary", sep = '_')))
    taQuery(select_query)

    ta.create(
      dataset,
      table = tolower(paste(session_id, "summary", sep = '_')),
      schemaName = "model_factory_cache",
      tableType = "dimension",
      row.names = TRUE
    )

    try(taQuery(
      sprintf(
        "DELETE FROM model_factory.model_summary WHERE session_id in ('%s')",
        paste(session_id,collapse='\',\'')
      )
    ),silent=TRUE)

    insert_query <- sprintf(
      "insert into model_factory.model_summary
      select '%s', \"variable\",\"mean\",\"sd\",\"median\",\"min\",\"max\",\"n\",\"n_na\"
      from model_factory_cache.%s",
      paste0(session_id),
      tolower(paste(session_id, "summary", sep = '_'))
    )

    taQuery(insert_query)

    select_query <- sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(paste(session_id, "summary", sep = '_')))
    taQuery(select_query)
  }
}


#' Pull the summary of existing session_id from model_factory.model_summary table
#'
#' @param session_id Vector of session id's
#' @examples
#' pullSummary(c('titanic_training_vecht499_20160629_105350'))
#' @return Summary of existing session_id
#' @export
pullSummary <- function(session_id) {
  check_session_id <-
    taQuery(
      sprintf(
        "select * from model_factory.model_summary where session_id in ('%s')",
        paste(session_id,collapse='\',\'')
      )
    )
  if (nrow(check_session_id) == 0)
  {
    stop(
      simpleError(
        "Given session_id is not present in model_factory.model_summary table"
      )
    )
  }
  else {
    summary <-
      taQuery(
        sprintf(
          "select * from model_factory.model_summary where session_id in ('%s')",
          paste(session_id,collapse='\',\'')
        )
      )
    return(summary)
  }
}



#' Get Test results for a model of Classification type (0/1 as output)
#'
#' @param scores Predicted probability based on the test set
#' @param labels Actual labels based on the test set
#' @examples
#' train <- ta.data.frame("select * from model_factory.titanic_train",sourceType="query")
#' test <- ta.data.frame("select * from model_factory.titanic_test",sourceType="query")
#'
#' formula <- "survived ~ sex + pclass + age + family + embarked + title"
#' ta_forest <- ta.forest(formula= formula, data = train,type = "classification", ntree = 100,mtry=4)
#' ta_forest_prediction <- ta.forest.predict(object = ta_forest, newdata=test, id.col="passengerid")
#'
#' forest_output <- ta_forest_prediction$retval
#' forest_output$pred <- ifelse(forest_output$prediction=='Survived',as.numeric(forest_output$confidence_upper),
#'                              1-as.numeric(forest_output$confidence_upper))
#'
#' test <- ta.pull(test)
#' getTestResults(forest_output$pred,test$survived_int)
#' @return Test results based on given scores and labels
#' @export
getTestResults <- function(scores,  labels) {
  if (class(scores) == "data.frame" && ncol(scores) > 1)
  {
    stop(
      simpleError(
        "The first parameter is a data frame, it should be a vector with the scores in it, and the second parameter should be the lables"
      )
    )
  }
  df <- data.frame(labels = labels,
                   scores = scores,
                   dummy = 1)
  num_customers <- nrow(df)
  num_target <- sum(df$labels)
  df %>% arrange(desc(scores)) %>%
    mutate(
      population = cumsum(dummy) / num_customers,
      target_population = cumsum(labels) / num_target,
      true_positives = cumsum(labels),
      false_positives = cumsum(dummy) - cumsum(labels),
      true_negatives = (num_customers - num_target - cumsum(dummy) +
                          cumsum(labels)),
      false_negatives = num_target - cumsum(labels)
    ) %>%
    select(
      scores,
      labels,
      population,
      target_population,
      true_positives,
      false_positives,
      true_negatives,
      false_negatives
    )
}

#' Get ROC curve table for a model of Classification type (0/1 as output)
#'
#' @param scores Predicted probability based on the test set
#' @param labels Actual labels based on the test set
#' @examples
#' train <- ta.data.frame("select * from model_factory.titanic_train",sourceType="query")
#' test <- ta.data.frame("select * from model_factory.titanic_test",sourceType="query")
#'
#' formula <- "survived ~ sex + pclass + age + family + embarked + title"
#' ta_forest <- ta.forest(formula= formula, data = train,type = "classification", ntree = 100,mtry=4)
#' ta_forest_prediction <- ta.forest.predict(object = ta_forest, newdata=test, id.col="passengerid")
#'
#' forest_output <- ta_forest_prediction$retval
#' forest_output$pred <- ifelse(forest_output$prediction=='Survived',as.numeric(forest_output$confidence_upper),
#'                              1-as.numeric(forest_output$confidence_upper))
#'
#' test <- ta.pull(test)
#' getROC(forest_output$pred,test$survived_int)
#' @return ROC curve table based on given scores and labels
#' @export
getROC <- function(scores,  labels) {
  if (class(scores) == "data.frame" && ncol(scores) > 1)
  {
    stop(
      simpleError(
        "The first parameter is a data frame, it should be a vector with the scores in it, and the second parameter should be the lables vector"
      )
    )
  }
  test_results <- getTestResults(scores, labels)
  roc_line <- data.frame(
    "population" = test_results$population,
    "true_positive_rate" = test_results$true_positives / (test_results$true_positives +
                                                     test_results$false_negatives),
    "false_positive_rate" = test_results$false_positives / (test_results$true_negatives +
                                                     test_results$false_positives)
  )
  return(roc_line)
}

#' Get liftchart table for a model of Classification type (0/1 as output)
#'
#' @param scores Predicted probability based on the test set
#' @param labels Actual labels based on the test set
#' @examples
#' train <- ta.data.frame("select * from model_factory.titanic_train",sourceType="query")
#' test <- ta.data.frame("select * from model_factory.titanic_test",sourceType="query")
#'
#' formula <- "survived ~ sex + pclass + age + family + embarked + title"
#' ta_forest <- ta.forest(formula= formula, data = train,type = "classification", ntree = 100,mtry=4)
#' ta_forest_prediction <- ta.forest.predict(object = ta_forest, newdata=test, id.col="passengerid")
#'
#' forest_output <- ta_forest_prediction$retval
#' forest_output$pred <- ifelse(forest_output$prediction=='Survived',as.numeric(forest_output$confidence_upper),
#'                              1-as.numeric(forest_output$confidence_upper))
#'
#' test <- ta.pull(test)
#' getLiftChart(forest_output$pred,test$survived_int)
#' @return Liftchart table based on given scores and labels
#' @export
getLiftChart <- function(scores,  labels) {
  if (class(scores) == "data.frame" && ncol(scores) > 1)
  {
    stop(
      simpleError(
        "The first parameter is a data frame, it should be a vector with the scores in it, and the second parameter should be the lables"
      )
    )
  }
  test_results <- getTestResults(scores, labels)
  liftchart_line <- data.frame(
    "population" = test_results$population,
    "target_population" = test_results$target_population
  )
  return(liftchart_line)
}

#' Get Confusion matrix for a model of Classification type (0/1 as output) with a given treshold and threshold_type
#'
#' @param scores Predicted probability based on the test set
#' @param labels Actual labels based on the test set
#' @param threshold_value threshold_value to choose
#' @param threshold_type Type of threshold to choose ("population" or "probability")
#' @examples
#' train <- ta.data.frame("select * from model_factory.titanic_train",sourceType="query")
#' test <- ta.data.frame("select * from model_factory.titanic_test",sourceType="query")
#'
#' formula <- "survived ~ sex + pclass + age + family + embarked + title"
#' ta_forest <- ta.forest(formula= formula, data = train,type = "classification", ntree = 100,mtry=4)
#' ta_forest_prediction <- ta.forest.predict(object = ta_forest, newdata=test, id.col="passengerid")
#'
#' forest_output <- ta_forest_prediction$retval
#' forest_output$pred <- ifelse(forest_output$prediction=='Survived',as.numeric(forest_output$confidence_upper),
#'                              1-as.numeric(forest_output$confidence_upper))
#'
#' test <- ta.pull(test)
#' getConfMatrix(forest_output$pred,test$survived_int,0.5,"probability")
#' getConfMatrix(forest_output$pred,test$survived_int,0.5,"population")
#' @return Confusion matrix based on given scores, labels, threshold_value and threshold type
#' @export
getConfMatrix <-
  function(scores, labels, threshold_value, threshold_type) {
    if (class(scores) == "data.frame" && ncol(scores) > 1)
    {
      stop(
        simpleError(
          "The first parameter is a data frame, it should be a vector with the scores in it, and the second parameter should be the lables"
        )
      )
    }

    test_results <- getTestResults(scores, labels)

    if (threshold_type == "population")
    {
      test_results1 <- subset(test_results, population <= threshold_value)
      test_results1 <- test_results1[nrow(test_results1),]
    }

    else if (threshold_type == "probability")
    {
      test_results1 <- subset(test_results, scores >= threshold_value)
      test_results1 <- test_results1[nrow(test_results1),]
    }
    actual_positives <-
      c(
        predicted_positives = test_results1$true_positives,
        predicted_negatives = test_results1$false_negatives
      )
    actual_negatives <-
      c(test_results1$false_positives,
        test_results1$true_negatives)
    return (rbind(actual_positives, actual_negatives))
  }

#' Get accuracy for a model of Classification type (0/1 as output) with a given treshold and threshold_type
#'
#' @param scores Predicted probability based on the test set
#' @param labels Actual labels based on the test set
#' @param threshold_value threshold_value to choose
#' @param threshold_type Type of threshold to choose ("population" or "probability")
#' @examples
#' train <- ta.data.frame("select * from model_factory.titanic_train",sourceType="query")
#' test <- ta.data.frame("select * from model_factory.titanic_test",sourceType="query")
#'
#' formula <- "survived ~ sex + pclass + age + family + embarked + title"
#' ta_forest <- ta.forest(formula= formula, data = train,type = "classification", ntree = 100,mtry=4)
#' ta_forest_prediction <- ta.forest.predict(object = ta_forest, newdata=test, id.col="passengerid")
#'
#' forest_output <- ta_forest_prediction$retval
#' forest_output$pred <- ifelse(forest_output$prediction=='Survived',as.numeric(forest_output$confidence_upper),
#'                              1-as.numeric(forest_output$confidence_upper))
#'
#' test <- ta.pull(test)
#' getAccuracy(forest_output$pred,test$survived_int,0.5,"probability")
#' getAccuracy(forest_output$pred,test$survived_int,0.5,"population")
#' @return Accuracy based on given scores, labels, threshold_value and threshold type
#' @export
getAccuracy <- function(scores, labels, threshold_value, threshold_type) {
  if (class(scores) == "data.frame" && ncol(scores) > 1)
  {
    stop(
      simpleError(
        "The first parameter is a data frame, it should be a vector with the scores in it, and the second parameter should be the lables"
      )
    )
  }
  test_results <- getTestResults(scores, labels)

  if (threshold_type == "population")
  {
    test_results1 <- subset(test_results, population <= threshold_value)
    test_results1 <- test_results1[nrow(test_results1),]
  }
  else if (threshold_type == "probability")
  {
    test_results1 <- subset(test_results, scores >= threshold_value)
    test_results1 <- test_results1[nrow(test_results1),]
  }
  else{
    stop(
      simpleError(
        "Only the threshold_types \"population\" and \"probability\" are supported"
      )
    )
  }

  accuracy = (test_results1$true_positives + test_results1$true_negatives) /
    (
      test_results1$true_positives + test_results1$true_negatives + test_results1$false_positives +
        test_results1$false_negatives
    )
  return(accuracy)
}


#' Write the test results to model_factory.model_test_results table
#'
#' @param dataset Dataframe which is the result of getTestResults to be added unto the summary table
#' @return The result of taQuery to update the model_factory.model_test_results table
#' @examples
#' model_id <- c('titanic_training')
#' getSessionId(model_id)
#' train <- ta.data.frame("select * from model_factory.titanic_train",sourceType="query")
#' test <- ta.data.frame("select * from model_factory.titanic_test",sourceType="query")
#'
#' formula <- "survived ~ sex + pclass + age + family + embarked + title"
#' ta_forest <- ta.forest(formula= formula, data = train,type = "classification", ntree = 100,mtry=4)
#' ta_forest_prediction <- ta.forest.predict(object = ta_forest, newdata=test, id.col="passengerid")
#'
#' forest_output <- ta_forest_prediction$retval
#' forest_output$pred <- ifelse(forest_output$prediction=='Survived',as.numeric(forest_output$confidence_upper),
#'                              1-as.numeric(forest_output$confidence_upper))
#'
#' test <- ta.pull(test)
#' test_results <- getTestResults(forest_output$pred,test$survived_int)
#' storeTestResults(test_results)
#' @export
storeTestResults <- function(dataset) {
  if (class(dataset) != "data.frame")
  {
    stop(
      simpleError(
        "Given dataset does not have class data.frame. Please give the data.frame from getTestResults() as input"
      )
    )
  }
  else {
    select_query <-
      sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(paste(session_id, "test_results", sep =
                                                                      '_')))
    taQuery(select_query)

    ta.create(
      dataset,
      table = tolower(paste(session_id, "test_results", sep = '_')),
      schemaName = "model_factory_cache",
      tableType = "dimension",
      row.names = TRUE
    )

    count_query <-
      sprintf(
        "select count(*)
        from model_factory.model_test_results
        where session_id = '%s'",
        paste0(session_id)
      )

    nr <- ta.Query(count_query)

    if(nr > 0){
      delete_query <-
        sprintf(
          "delete from model_factory.model_test_results
          where session_id = '%s'",
          paste0(session_id)
        )

      ta.Query(delete_query)
    }

    insert_query <-
      sprintf(
        "insert into model_factory.model_test_results
        select '%s', scores, labels, population, target_population,
        true_positives, false_positives, true_negatives, false_negatives
        from model_factory_cache.%s",
        paste0(session_id),
        tolower(paste(session_id, "test_results", sep = '_'))
      )

    taQuery(insert_query)

    select_query <-
      sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(paste(session_id, "test_results", sep =
                                                                       '_')))
    taQuery(select_query)
  }
}


#' Pull test results from model_factory.model_test_results table based on session_id
#'
#' @param session_id Vector of session id's
#' @examples
#' pullTestResults(c('titanic_training_vecht499_20160630_623433'))
#' @return Test results of existing session_id
#' @export
pullTestResults <- function(session_id) {
  check_session_id <-
    taQuery(
      sprintf(
        "select * from model_factory.model_test_results where session_id in ('%s')",
        paste(session_id,collapse='\',\'')
      )
    )
  if (nrow(check_session_id) == 0) {
    stop(
      simpleError(
        "Given session_id is not present in model_factory.model_test_results table"
      )
    )
  } else {
    test_results <-
      taQuery(
        sprintf(
          "select * from model_factory.model_test_results where session_id in ('%s')",
          paste(session_id,collapse='\',\'')
        )
      )
    return(test_results)
  }
}

#' Pull ROC from model_factory.model_test_results table based on session_id
#'
#' @param session_id Vector of session id's
#' @examples
#' pullROC(c('titanic_training_vecht499_20160630_623433'))
#' @return ROC table of existing session_id
#' @export
pullROC <- function(session_id) {
  check_session_id <-
    taQuery(
      sprintf(
        "select * from model_factory.model_test_results where session_id in ('%s')",
        paste(session_id,collapse='\',\'')
      )
    )
  if (nrow(check_session_id) == 0) {
    stop(
      simpleError(
        "Given session_id is not present in model_factory.model_test_results table"
      )
    )
  } else {
    test_results <-
      taQuery(
        sprintf(
          "select * from model_factory.model_test_results where session_id in ('%s')",
          paste(session_id,collapse='\',\'')
        )
      )
    roc_line <- data.frame(
      "session_id" = test_results$session_id,
      "population" = test_results$population,
      "true_positive_rate" = test_results$true_positives /
        (
          test_results$true_positives + test_results$false_negatives
        ),
      "false_positive_rate" = test_results$false_positives /
        (
          test_results$true_negatives + test_results$false_positives
        )
    )
    return(roc_line)
  }
}


#' Pull Lift chart from model_factory.model_test_results table based on session_id
#'
#' @param session_id Vector of session id's
#' @examples
#' pullLiftChart(c('titanic_training_vecht499_20160630_623433'))
#' @return Lift chart table of existing session_id
#' @export
pullLiftChart <- function(session_id) {
  check_session_id <-
    taQuery(
      sprintf(
        "select * from model_factory.model_test_results where session_id in ('%s')",
        paste(session_id,collapse='\',\'')
      )
    )
  if (nrow(check_session_id) == 0) {
    stop(
      simpleError(
        "Given session_id is not present in model_factory.model_test_results table"
      )
    )
  } else {
    test_results <-
      taQuery(
        sprintf(
          "select * from model_factory.model_test_results where session_id in ('%s')",
          paste(session_id,collapse='\',\'')
        )
      )
    liftchart_line <-
      data.frame(
        "session_id" = test_results$session_id,
        "population" = test_results$population,
        "target_population" = test_results$target_population
      )
    return(liftchart_line)
  }
}


#' Pull Confusion matrix from model_factory.model_test_results table based on session_id, threshold_value and threshold_type
#'
#' @param session_id Existing session_id
#' @param threshold_value Threshold_value
#' @param threshold_type Threshold type ("population" or "probability")
#' @examples
#' pullConfMatrix('titanic_training_vecht499_20160630_623433',0.5,"population")
#' pullConfMatrix('titanic_training_vecht499_20160630_623433',0.5,"probability")
#' @return Confusion matrix of existing session_id defined by given threshold_value and threshold_type
#' @export
pullConfMatrix <- function(session_id, threshold_value, threshold_type) {
  check_session_id <-
    taQuery(
      sprintf(
        "select * from model_factory.model_test_results where session_id='%s'",
        session_id
      )
    )
  if (nrow(check_session_id) == 0)
  {
    stop(
      simpleError(
        "Given session_id is not present in model_factory.model_test_results table"
      )
    )
  }
  else {
    test_results <-
      taQuery(
        sprintf(
          "select * from model_factory.model_test_results where session_id='%s'",
          session_id
        )
      )
    if (threshold_type == "population")
    {
      test_results1 <- subset(test_results, population >= threshold_value)
      p <- min(test_results1$population - threshold_value) + threshold_value
      test_results1 <- subset(test_results1, population == p)
    }

    else if (threshold_type == "probability")
    {
      test_results1 <- subset(test_results, score >= threshold_value)
      p <- min(test_results1$score - threshold_value) + threshold_value
      test_results1 <- subset(test_results1, score == p)
    }
    else{
      stop(
        simpleError(
          "Only the threshold_types \"population\" and \"probability\" are supported"
        )
      )
    }
    actual_positives <-
      c(
        predicted_positives = test_results1$true_positives,
        predicted_negatives = test_results1$false_negatives
      )
    actual_negatives <-
      c(test_results1$false_positives,
        test_results1$true_negatives)
    return (rbind(actual_positives, actual_negatives))
  }
}


#' Pull Accuracy from model_factory.model_test_results table based on session_id, threshold_value and threshold_type
#'
#' @param session_id Existing session_id
#' @param threshold_value Threshold_value
#' @param threshold_type Threshold type ("population" or "probability")
#' @examples
#' pullAccuracy('titanic_training_vecht499_20160630_623433',0.5,"population")
#' pullAccuracy('titanic_training_vecht499_20160630_623433',0.5,"probability")
#' @return Accuracy of existing session_id defined by given threshold_value and threshold_type
#' @export
pullAccuracy <- function(session_id, threshold_value, threshold_type) {
  check_session_id <-
    taQuery(
      sprintf(
        "select * from model_factory.model_test_results where session_id='%s'",
        session_id
      )
    )
  if (nrow(check_session_id) == 0)
  {
    stop(
      simpleError(
        "Given session_id is not present in model_factory.model_test_results table"
      )
    )
  }
  else {
    test_results <-
      taQuery(
        sprintf(
          "select * from model_factory.model_test_results where session_id='%s'",
          session_id
        )
      )
    if (threshold_type == "population")
    {
      test_results1 <- subset(test_results, population >= threshold_value)
      p <- min(test_results1$population - threshold_value) + threshold_value
      test_results1 <- subset(test_results1, population == p)
    }

    else if (threshold_type == "probability")
    {
      test_results1 <- subset(test_results, score >= threshold_value)
      p <- min(test_results1$score - threshold_value) + threshold_value
      test_results1 <- subset(test_results1, score == p)
    }
    else{
      stop(
        simpleError(
          "Only the threshold_types \"population\" and \"probability\" are supported"
        )
      )
    }
    accuracy = (test_results1$true_positives + test_results1$true_negatives) /
      (
        test_results1$true_positives + test_results1$true_negatives + test_results1$false_positives +
          test_results1$false_negatives
      )
    return(accuracy)
  }
}


#' Write the scores to model_factory.model_scores table
#'
#' @param id Customer identifyer
#' @param scores Scores
#' @examples
#' model_id <- c('titanic_training')
#' getSessionId(model_id)
#' train <- ta.data.frame("select * from model_factory.titanic_train",sourceType="query")
#' test <- ta.data.frame("select * from model_factory.titanic_test",sourceType="query")
#' formula <- "survived ~ sex + pclass + age + family + embarked + title"
#' ta_forest <- ta.forest(formula= formula, data = train,type = "classification", ntree = 100,mtry=4)
#' ta_forest_prediction <- ta.forest.predict(object = ta_forest, newdata=test, id.col="passengerid")
#' forest_output <- ta_forest_prediction$retval
#' forest_output$pred <- ifelse(forest_output$prediction=='Survived',as.numeric(forest_output$confidence_upper),
#'                              1-as.numeric(forest_output$confidence_upper))
#' test <- ta.pull(test)
#' storeModelScores(test$passengerid,forest_output$pred)
#' @return The result of taQuery to update the model_factory.model_scores table
#' @export
storeModelScores <- function(id, scores, scores_class = NA) {
  dataset <- data.frame(id = id, scores = scores, scores_class = scores_class)
  select_query <-
    sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(paste(session_id, "scores", sep =
                                                                           '_')))
  taQuery(select_query)
  
  ta.create(
    dataset,
    table = tolower(paste(session_id, "scores", sep = '_')),
    schemaName = "model_factory_cache",
    tableType = "dimension",
    row.names = TRUE
  )
  
  try(taQuery(
    sprintf(
      "DELETE FROM model_factory.model_scores WHERE session_id in ('%s')",
      paste(session_id,collapse='\',\'')
    )
  ),silent=TRUE)
  
  insert_query <- sprintf(
    "insert into model_factory.model_scores
    select '%s' as session_id, id, scores, scores_class
    from model_factory_cache.%s",
    paste0(session_id),
    tolower(paste(session_id, "scores", sep = '_'))
  )
  
  taQuery(insert_query)
  
  select_query <-
    sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(paste(session_id, "scores", sep =
                                                                           '_')))
  taQuery(select_query)
}

#' Pull model scores from model_factory.model_scores table based on session_id
#'
#' @param session_id Vector of session id's
#' @examples
#' pullModelScores(c('titanic_training_vecht499_20160630_623433'))
#' @return Model scores of existing session_id
#' @export
pullModelScores <- function(session_id) {
  check_session_id <-
    taQuery(
      sprintf(
        "select * from model_factory.model_scores where session_id in ('%s')",
        paste(session_id,collapse='\',\'')
      )
    )
  if (nrow(check_session_id) == 0)
  {
    stop(simpleError(
      "Given session_id is not present in model_factory.model_scores table"
    ))
  }
  else {
    model_scores <-
      taQuery(
        sprintf(
          "select * from model_factory.model_scores where session_id in ('%s')",
          paste(session_id,collapse='\',\'')
        )
      )
    return(model_scores)
  }
}


#' Store the backtesting data for a time series model.
#'
#' @param predicted_value The predictions
#' @param actual_value The the real value that the model was trying to predict
#' @param period The time axis (POSIXlt)
#' @examples
#' model_id <- c('example_package')
#' addModelId(model_id,'Example for ModelFactoryR package','aggregated')
#' getSessionId(model_id)
#' df <- data.frame("predicted_value"=c(1.5,2.3,3.1,4.9,5.7),"actual_value"=c(1.2,3.0,3.7,4.3,5.1),
#' "period"= as.POSIXlt(c('2015-01-01','2015-01-02','2015-01-03','2015-01-04','2015-01-05'),"Europe/Amsterdam"))
#' storeBackTestCoordinates(df$predicted_value,df$actual_value,df$period)
#' @return The coordenates for a backtesting plot
#' @export
storeBackTestCoordinates <-
  function(predicted_value, actual_value, period) {
    if (class(period) != "POSIXlt"){
      stop(simpleError("Period is not of class POSIXlt"))
    }

    if (length(predicted_value) != length(actual_value) || length(predicted_value) != length(period)) {
      stop(simpleError("The input vectores are not of equal length"))
    }

    pred_table <-
      data.frame(
        predicted_value = predicted_value,
        actual_value = actual_value,
        period = period
      )
    taQuery(sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(
      paste(session_id, "backtesting", sep = '_')
    )))

    try(taQuery(
      sprintf(
        "DELETE FROM model_factory.model_backtesting WHERE session_id in ('%s')",
        paste(session_id,collapse='\',\'')
      )
    ),silent=TRUE)

    ta.create(
      pred_table,
      table = tolower(paste(session_id, "backtesting", sep = '_')),
      schemaName = "model_factory_cache",
      tableType = "dimension",
      row.names = FALSE,
      colTypes=c("VARCHAR(50)","NUMERIC(28,6)","NUMERIC(28,6)","TIMESTAMP")
    )

    insert_query <-
      sprintf(
        "insert into  model_factory.model_backtesting
        select '%s' as session_id,predicted_value,actual_value, period
        from model_factory_cache.%s",
        paste0(session_id),
        tolower(paste(session_id, "backtesting", sep = '_'))
      )

    taQuery(insert_query)

    taQuery(sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(
      paste(session_id, "backtesting", sep = '_')
    )))
  }

#' Pull backtesting data from model_factory.model_backtesting table based on session_id
#'
#' @param session_id Vector of session id's
#' @examples
#' pullBackTestCoordinates(c('example_package_vecht499_20160630_351931'))
#' @return Model backtest results of existing session_id
#' @export
pullBackTestCoordinates <- function(session_id) {
  check_session_id <-
    taQuery(
      sprintf(
        "select * from model_factory.model_backtesting where session_id in ('%s')",
        paste(session_id,collapse='\',\'')
      )
    )
  if (nrow(check_session_id) == 0)
  {
    stop(
      simpleError(
        "Given session_id is not present in model_factory.model_backtesting table"
      )
    )
  }
  else {
    backtest <-
      taQuery(
        sprintf(
          "select * from model_factory.model_backtesting where session_id in ('%s')",
          paste(session_id,collapse='\',\'')
        )
      )
    return(backtest)
  }
}


#' Get variable importance from a given model
#'
#' @param model_type Type of model ("glm","ta.glm","randomForest","ta.forest")
#' @param model Your model of type lm/glm/ta.glm/ta.forest/randomforest
#' @examples
#' train_tadf <- ta.data.frame("select * from model_factory.titanic_train",sourceType="query")
#' train <- ta.pull(train_tadf)
#'
#' formula_glm <- as.formula(survived_int ~ sex + pclass + age + family + embarked + title)
#' mod <- glm(formula_glm,train,family="binomial")
#' getVarimp(model_type="glm",mod)
#'
#' formula <- as.formula(survived ~ sex + pclass + age + family + embarked + title)
#' mod <- randomForest(formula,data= train,ntree=100,mtry=4,keep.forest=TRUE)
#' getVarimp(model_type="randomForest",mod)
#'
#' formula_glm <- as.formula(survived_int ~ sex + pclass + age + family + embarked + title)
#' mod <- ta.glm(formula= formula_glm, data = train_tadf,family=binomial())
#' getVarimp(model_type="ta.glm",mod)
#'
#' formula <- as.formula(survived ~ sex + pclass + age + family + embarked + title)
#' mod <- ta.forest(formula= formula, data = train,type = "classification", ntree = 100,mtry=4)
#' getVarimp(model_type="ta.forest",mod)
#' @return Dataframe with variable importance data
#' @export
getVarimp <- function(model_type,model)

{
  suppressWarnings(if (model_type == "glm" &
                       (class(model) != "lm" &
                        class(model) != "glm"))
  {
    stop(simpleError("Given model is not of type glm"))
  }

  else if (model_type == "randomForest" &
           class(model) != "randomForest.formula")
  {
    stop(simpleError("Given model is not of type randomForest"))
  }

  else if (model_type == "ta.forest" & class(model) != "ta.forest")
  {
    stop(simpleError("Given model is not of type ta.forest"))
  }

  else if (model_type == "ta.glm" & class(model) != "ta.glm")
  {
    stop(simpleError("Given model is not of type ta.glm"))
  }

  else
  {
    if (model_type == "glm" &
        (class(model) == "lm" | class(model) == "glm"))
    {
      x <- data.frame(
        "variable" = rownames(summary(model)$coefficients),
        "importance" = summary(model)$coefficients[,4],
        "std" = summary(model)$coefficients[,2],
        "coefficients" = summary(model)$coefficients[,1]
      )
      varimp <- data.frame(x,row.names = NULL)
      varimp <- varimp[order(varimp$importance,decreasing = FALSE),]
      return(varimp)
    }

    else if (model_type == "randomForest" &
             class(model) == "randomForest.formula")
    {
      x <- as.data.frame(importance(model))
      x$variable = rownames(x)
      varimp <- data.frame(
        "variable" = x$variable,
        "importance" = x$MeanDecreaseGini,
        "std" = NA,
        "coefficients" = NA,
        row.names = NULL
      )
      varimp <- varimp[order(varimp$importance,decreasing = TRUE),]
      return(varimp)
    }

    else if (model_type == "ta.forest" &
             class(model) == "ta.forest")
    {
      forest_evaluate <- ta.forest.evaluate(model)
      importances <- ta.pull(forest_evaluate$retval)

      x <-
        importances %>% mutate(importance = as.numeric(importance)) %>% group_by(variable) %>%
        summarize(importance = mean(importance)) %>% arrange(desc(importance)) %>%
        select(variable,importance)
      varimp <- data.frame(
        "variable" = x$variable,
        "importance" = x$importance,
        "std" = NA,
        "coefficients" = NA,row.names = NULL
      )
      return(varimp)
    }

    else if (model_type == "ta.glm" & class(model) == "ta.glm")
    {
      x <- ta.pull(model$result)
      x <- x[0:(which(x$predictor == 'ITERATIONS #') - 1),]
      varimp <- data.frame(
        "variable" = x$predictor,
        "importance" = x$p_value,
        "std" = x$std_error,
        "coefficients" = x$estimate
      )
      varimp <- varimp[order(varimp$importance,decreasing = FALSE),]
      return(varimp)
    }

  })
}



#' Store a dataframe/vectors filled with the variable importance of the model.
#' Standard deviation and coefficients are optional
#'
#'
#' @param session_id The current session ID
#' @param variable The variables
#' @param importance The importance of the variables
#' @param std The standard diviation
#' @param coefficients The coefficients
#' @examples
#' model_id <- c('titanic_training')
#' getSessionId(model_id)
#'
#' train <- ta.data.frame("select * from model_factory.titanic_train",sourceType="query")
#' test <- ta.data.frame("select * from model_factory.titanic_test",sourceType="query")
#'
#' formula <- "survived ~ sex + pclass + age + family + embarked + title"
#' ta_forest <- ta.forest(formula= formula, data = train,type = "classification", ntree = 100,mtry=4)
#' varimp <- getVarimp("ta.forest",ta_forest)
#' storeVarImp(varimp$variable,varimp$importance)
#' @return The result of the insert taQuery
#' @export
storeVarImp <- function(variable,importance,std,coefficients) {
  if (missing(std))
    std <- as.numeric(NA)
  if (missing(coefficients))
    coefficients <- as.numeric(NA)
  varimp <-
    data.frame(
      variable = variable,importance = importance,std = std,coefficients = coefficients
    )

  taQuery(sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(paste(
    session_id, "varimp", sep = '_'
  ))))

  ta.create(
    varimp,table = tolower(paste(session_id,"varimp",sep = '_')),schemaName =
      "model_factory_cache",tableType = "dimension",row.names = TRUE
  )

  try(
    taQuery(sprintf(
      "DELETE FROM model_factory.model_varimp WHERE session_id = '%s'",paste0(session_id)
    )
  ),silent=TRUE)


  insert_query <- sprintf(
    "insert into model_factory.model_varimp
    select '%s',variable,importance,std,coefficients
    from model_factory_cache.%s",paste0(session_id),
    tolower(paste(session_id,"varimp",sep = '_'))
  )

  taQuery(insert_query)

  taQuery(sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(paste(
    session_id, "varimp", sep = '_'
  ))))
}

#' Pull the variable importance of existing session_id from model_factory.model_varimp table
#'
#' @param session_id Vector of session id's
#' @examples
#' pullVarimp(c('titanic_training_vecht499_20160630_364493'))
#' @return Summary of existing session_id
#' @export
pullVarimp <- function(session_id) {
  check_session_id <-
    taQuery(
      sprintf(
        "select * from model_factory.model_varimp where session_id in ('%s')",
        paste(session_id,collapse='\',\'')
      )
    )
  if (nrow(check_session_id) == 0)
  {
    stop(simpleError(
      "Given session_id is not present in model_factory.model_varimp table"
    ))
  }
  else {
    summary <-
      taQuery(
        sprintf(
          "select * from model_factory.model_varimp where session_id in ('%s')",
          paste(session_id,collapse='\',\'')
        )
      )
    return(summary)
  }
}

#' Returns all columns from a given table that have been cleared to be used by models
#'
#' @param get.test.columns.also If set to TRUE columns that have test_column =1 will also be added
#' @examples
#' formula <- as.formula(paste("churn_indicator ~ " paste(getValidColumns(schema="model_factory.", table_name="bom_loc"),collapse=" + "),sep=""))
#' @return The result of taQuery to execute the installed R script. Session_id is returned as result as well
#' @export
getValidColumns <- function(get.test.columns.also=FALSE) {
  if (get.test.columns.also){
    return(taQuery("SELECT column_name from model_factory.input_data_valid_columns WHERE table_name = %s AND (valid_column = 1 OR test_column = 1)"))
  }else{
    return(taQuery("SELECT column_name from model_factory.input_data_valid_columns WHERE table_name = %s AND valid_columns = 1"))
  }
}


#' Stores a model object as rows in a the Aster Database.
#' The object must be serializeable, so vectors, lists, dataframes
#'
#' @param model The model object to store
#' @examples
#' titanic_dataset <- taQuery("select * from modelfactory.titanic_dataset")
#' train <- titanic_dataset[titanic_dataset$train==1,]
#' formula <- as.formula(survived ~ sex + pclass + age + family + embarked + title)
#' forest <- randomForest(formula,data= train,ntree=100,mtry=4,keep.forest=TRUE)
#' storeModel(forest)
#' @return The result of the clean up
#' @export
storeModel <- function(model) {
  tryCatch({
    t_as_bin <- serialize(model,connection = NULL)
    t_as_bin_compressed <- memCompress(t_as_bin,"gzip")
    t_as_bin_df <- data.frame(model=t_as_bin_compressed)

    # Have to create temp tabe because ta.pull can only pull tables, ta.data.frames did not work
    taQuery(sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(paste(session_id, "model", sep = '_'))))
    ta.create(t_as_bin_df,table=tolower(paste(session_id,"model",sep = '_')),schemaName="model_factory_cache",
              tableType = "dimension",row.names = FALSE,colTypes=c(model="bytea"))

    insert_query <- sprintf(
      "insert into model_factory.model_store
    select '%s',model
    from model_factory_cache.%s",paste0(session_id),
      tolower(paste(session_id,"model",sep = '_'))
    )

    taQuery(insert_query)

    taQuery(sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(paste(
      session_id, "model", sep = '_'
    ))))

  }, error = function(e) {
    warning(
      simpleError(
        sprintf("There was a problem when storing the model %s",e)
      )
    )
  },finally = {
    taQuery(sprintf("DROP TABLE if exists model_factory_cache.%s", tolower(paste(
      session_id, "model", sep = '_'
    ))))
  })
}

#' Retrives a model object stored with storeModel() function in session with session ID session_id.
#'
#' @param session_id_in The session Id of the session that stored the model
#' @examples
#' forest <- pullModel("ols_session_id")
#' varimp <- getVarimp("randomForest",forest)
#' forest_prediction <-  as.data.frame(predict(forest,newdata= test,"prob"))
#' @return The result of the clean up
#' @export
pullModel <- function(session_id_in){
  tryCatch({
    # Have to create temp tabe because ta.pull can only pull tables, ta.data.frames did not work
    taQuery(sprintf("DROP TABLE if exists model_factory.%s", tolower(paste(session_id_in, "model", sep = '_'))))
    taQuery(sprintf("CREATE DIMENSION TABLE model_factory.%s AS (SELECT model FROM model_factory.model_strore WHERE session_id = '%s')",tolower(paste(session_id_in,"model",sep = '_')),session_id_in))
    t_as_bin_df2 <- ta.pull(source=tolower(paste(session_id_in,"model",sep = '_')),schemaName = "model_factory",row.names = FALSE,colClasses = c("raw"))
    t_as_bin_compressed2 <- t_as_bin_df2[,1]
    t_as_bin2 <- memDecompress(t_as_bin_compressed2,"gzip")

    return(unserialize(t_as_bin2))
  }, error = function(e) {
    warning(
      simpleError(
        sprintf("There was a problem when retreaving the model %s",e)
      )
    )
  },finally = {
    taQuery(sprintf("DROP TABLE if exists model_factory.%s", tolower(paste(
      session_id_in, "model", sep = '_'
    ))))
  })
}


#' Run script on Aster cluster
#'
#' @param script Script you want to run on Aster
#' @param mem The amount of memory that the R session is allowed ot use id MB, default is unlimited
#' @examples
#' ta.install.scripts('C:/Users/vecht499/Desktop/test.R')
#' runOnAster('test.R')
#' runOnAster('test.R','1000')
#' @return The result of taQuery to execute the installed R script. Session_id is returned as result as well
#' @export
runOnAster <- function(script, mem = "unlimited")
{
  query <- sprintf(
    "SELECT * FROM stream(
    ON (select '%s' as user_aster,
    '%s' as password_aster,
    '%s' as user_teradata,
    '%s' as password_teradata)
    PARTITION BY 1
    SCRIPT('%s')
    OUTPUTS('session_id varchar(255)')
    MEM_LIMIT_MB ('%s'))",
    getOption("kpnConfig")$aster$username,
    getOption("kpnConfig")$aster$password,
    getOption("kpnConfig")$teradata$username,
    getOption("kpnConfig")$teradata$password,
    paste('Rexec',script,sep = " "),
    mem
  )

  x <- taQuery(query)
  return(substring(tail(x,1)$session_id, 3))
}

#' Installes the script on the Aster cluster before running it.
#' It will remove any previous script with the same name
#'
#' @param path The path to the script file with the trailing slash
#' @param script Script you want to run on Aster
#' @param mem The amount of memory that the R session is allowed ot use id MB, default is unlimited
#' @examples
#' installAndRunOnAster('C:/projects/awesome_model/','save_kpn.R')
#' or
#' installAndRunOnAster('C:/projects/awesome_model/','save_kpn_with_less_memory.R','1000')
#' @return The result of taQuery to execute the installed R script. Session_id is returned as result as well
#' @export
installAndRunOnAster <- function(path, script, mem = "unlimited")
{
  try(ta.remove.scripts(script, stopOnError = FALSE),silent = TRUE)
  ta.install.scripts(paste(path,script,sep=""))
  o <- runOnAster(script, mem)
  try(ta.remove.scripts(script, stopOnError = FALSE),silent = TRUE)
  return(o)
}

#' Renames existing model and modifies all related session_id's
#'
#' @param old_model_id Model_id you want to change
#' @param new_model_id Model_id you want to use
#' @examples
#' renameModel('training','titanic_training')
#' @return The result of taQuery to rename the model and modify all related session id's
#' @export
renameModel <- function(old_model_id,new_model_id) {
  config <- getOption("kpnConfig")

  check_old_model_id <-
    taQuery(
      sprintf(
        "select * from model_factory.model_overview where model_id='%s'",
        old_model_id
      )
    )
  
  check_old_model_id1 <-
    taQuery(
      sprintf(
        "select * from model_factory.run_history where model_id='%s'",
        old_model_id
      )
    )

  check_new_model_id <-
    taQuery(
      sprintf(
        "select * from model_factory.model_overview where model_id='%s'",
        new_model_id
      )
    )

  if (nrow(check_old_model_id) == 0)
  {
    stop(
      simpleError(
        "Given old_model_id is not present in model_factory.model_overview table, therefore can't be renamed"
      )
    )
  }

  if (nrow(check_new_model_id) > 0)
  {
    stop(
      simpleError(
        "Given new_model_id is already present in model_factory.model_overview table, therefore can't be used for renaming"
      )
    )
  }

  
  if (nrow(check_old_model_id) > 0 & nrow(check_old_model_id1) == 0)
  {
    try(taQuery(
      sprintf(
        "Update model_factory.model_overview set model_id= '%s' where model_id='%s'",
        new_model_id, old_model_id
      )
    ),silent = TRUE)
  }
  
  else
  {
    try(taQuery(
      sprintf(
        "update model_factory.model_scores
        set session_id= replace(session_id,'_%s_','_%s_')
        where session_id like '%%_%s_%%'",
        old_model_id,new_model_id,old_model_id
      )
    ),silent = TRUE)

    try(taQuery(
      sprintf(
        "update model_factory.metadata_table
        set session_id= replace(session_id,'_%s_','_%s_')
        where session_id like '%%_%s_%%'",
        old_model_id,new_model_id,old_model_id
      )
    ),silent = TRUE)

    try(taQuery(
      sprintf(
        "update model_factory.model_backtesting
        set session_id= replace(session_id,'_%s_','_%s_')
        where session_id like '%%_%s_%%'",
        old_model_id,new_model_id,old_model_id
      )
    ),silent = TRUE)

    try(taQuery(
      sprintf(
        "update model_factory.model_summary
        set session_id= replace(session_id,'_%s_','_%s_')
        where session_id like '%%_%s_%%'",
        old_model_id,new_model_id,old_model_id
      )
    ),silent = TRUE)

    try(taQuery(
      sprintf(
        "update model_factory.model_store
        set session_id= replace(session_id,'_%s_','_%s_')
        where session_id like '%%_%s_%%'",
        old_model_id,new_model_id,old_model_id
      )
    ),silent = TRUE)

    try(taQuery(
      sprintf(
        "update model_factory.model_test_results
        set session_id= replace(session_id,'_%s_','_%s_')
        where session_id like '%%_%s_%%'",
        old_model_id,new_model_id,old_model_id
      )
    ),silent = TRUE)

    try(taQuery(
      sprintf(
        "update model_factory.model_varimp
        set session_id= replace(session_id,'_%s_','_%s_')
        where session_id like '%%_%s_%%'",
        old_model_id,new_model_id,old_model_id
      )
    ),silent = TRUE)


    try(taQuery(
      sprintf(
        "insert into model_factory.run_history
        select replace(session_id,'_%s_','_%s_') as session_id, user_id, '%s' as model_id, start_time, end_time, last_exported
        from model_factory.run_history where model_id='%s'",old_model_id,new_model_id,new_model_id, old_model_id
      )
    ),silent = TRUE)

    try(taQuery(
      sprintf(
        "delete from model_factory.run_history where model_id='%s'",old_model_id
      )
    ),silent = TRUE)


    try(taQuery(
      sprintf(
        "Update model_factory.model_overview set model_id= '%s' where model_id='%s'",
        new_model_id, old_model_id
      )
    ),silent = TRUE)
  }
}
