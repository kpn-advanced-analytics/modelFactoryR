library(RUnit)
library(modelFactoryR)
library(TeradataAsterR)

test.addModelId <- function(){
  try(taQuery("DELETE FROM model_factory.model_overview WHERE model_id = 'test_model_id'"),silent = TRUE)
  addModelId(model_id = "test_model_id",model_description = 'Temporary model_id for test script, you can safly remove this',score_id_type = 'test')
  row <- taQuery("SELECT * FROM model_factory.model_overview WHERE model_id = 'test_model_id'")
  checkTrue(nrow(row) > 0,"addModelId did not add a row in model_overview")
  checkTrue(nrow(row) < 2,"addModelId add more then one row in model_overview")
  checkTrue(row$model_description == 'Temporary model_id for test script, you can safly remove this',paste("Model description was not stored correctly:", row$model_description))
  checkTrue(row$score_id_type == 'test',"Model score_id_type was not stored correctly:" + row$score_id_type)

  checkException(addModelId(model_id = "test_model_id",
                            model_description = 'Temporary model_id for test script, you can safly remove this',
                            score_id_type = 'test'),
                 "addModelId did not throw an exception when creating a second model with the same ID")

  taQuery("DELETE FROM model_factory.model_overview WHERE model_id = 'test_model_id'")
}

test.getSessionId <- function(){
  try(taQuery("DELETE FROM model_factory.model_overview WHERE model_id = 'test_model_id'"),silent = TRUE)
  checkException(getSessionId("test_model_id"))
  try(taQuery("DELETE FROM model_factory.run_history WHERE session_id LIKE 'test_model_id%'"),silent = TRUE)

  session_id <<- "not_set"
  checkTrue(session_id == "not_set","Counld not_set session_id")
  try(taQuery("DELETE FROM model_factory.model_overview WHERE model_id = 'test_model_id'"),silent = TRUE)
  taQuery("INSERT INTO model_factory.model_overview (model_id, model_description, score_id_type, threshold, threshold_type) VALUES ('test_model_id','Temporary model_id for test script, you can safly remove this','test','test','test')")
  getSessionId("test_model_id")
  checkTrue(session_id != "not_set","session_id was not_set")
  checkTrue(grepl("test_model_id",session_id),"session id does not contain model ID")
  checkTrue(grepl(getOption("kpnConfig")$aster$username,session_id),"session id does not contain user name")

  row <- taQuery(paste("SELECT * FROM model_factory.run_history WHERE session_id ='", session_id,"'",sep = ""))
  checkTrue(nrow(row) > 0,"getSessionId did not add a row in run_history")
  checkTrue(nrow(row) < 2,"getSessionId add more then one row in run_history")
  checkTrue(row$model_id == "test_model_id","getSessionId did not_set the model ID correctly")
  checkTrue(row$user_id == getOption("kpnConfig")$aster$username,paste("getSessionId did not_set the user ID correctly:",row$user_id))
  checkTrue(is.null(row$threshold),paste("getSessionId did not_set the threshold correctly:",row$threshold))
  checkTrue(is.null(row$threshold_type),paste("getSessionId did not_set the threshold correctly:",row$threshold_type))


  taQuery("DELETE FROM model_factory.model_overview WHERE model_id='test_model_id'")
  taQuery(sprintf("DELETE FROM model_factory.run_history WHERE session_id = '%s'",session_id))
}

test.updateThreshold <- function(){
  try(taQuery("DELETE FROM model_factory.model_overview WHERE model_id = 'test_model_id'"),silent = TRUE)
  taQuery("INSERT INTO model_factory.model_overview (model_id, model_description, score_id_type) VALUES ('test_model_id','Temporary model_id for test script, you can safly remove this','test')")
  checkException(updateThreshold('test_model_id',threshold = 'test', threshold_type = 'test'),"updateThreshold did not complain about the threshold_type not being population or probability")
  updateThreshold('test_model_id',threshold = 'test', threshold_type = 'population')
  row <- taQuery("SELECT * FROM model_factory.model_overview WHERE model_id = 'test_model_id'")
  checkTrue(nrow(row) > 0,"updateThreshold removed the model row")
  checkTrue(nrow(row) < 2,"updateThreshold add a model row")
  checkTrue(row$model_description == 'Temporary model_id for test script, you can safly remove this',paste("updateThreshold description was not stored correctly:", row$model_description))
  checkTrue(row$score_id_type == 'test',paste("updateThreshold score_id_type was not stored correctly:", row$score_id_type))
  checkTrue(row$threshold == 'test',paste("updateThreshold threshold was not stored correctly:",row$threshold))
  checkTrue(row$threshold_type == 'population',paste("updateThreshold threshold_type was not stored correctly:", row$threshold_type))
  taQuery("DELETE FROM model_factory.model_overview WHERE model_id='test_model_id'")
}

test.closeSession <- function(){
  session_id <<- "not_set"
  #checkException(closeSession(),"closeSession did not throw an exception for a non existing session ID")

  try(sprintf("DELETE FROM model_factory.run_history WHERE session_id='%s'",session_id),silent = TRUE)
  taQuery(sprintf("INSERT INTO model_factory.run_history (session_id, user_id, model_id, start_time) VALUES ('%s','test_user','test_model_id',now())",session_id,sep = ""))
  closeSession()

  row <- taQuery(sprintf("SELECT * FROM model_factory.run_history WHERE session_id ='%s'",session_id))
  checkTrue(nrow(row) > 0,"closeSession removed run_history row")
  checkTrue(nrow(row) < 2,"closeSession added run_history row")
  checkTrue(row$user_id == "test_user",paste("closeSession changed user ID",row$user_id))
  checkTrue(row$model_id == "test_model_id",paste("closeSession changed the model ID",row$model_id ))
  checkTrue(!is.null(row$start_time),paste("closeSession set the start_time to null:",row$start_time))
  checkTrue(!is.null(row$end_time),paste("closeSession did not_set the end_time correctly:",row$end_time))
  taQuery(sprintf("DELETE FROM model_factory.run_history WHERE session_id = '%s'",session_id))
}

test.deleteModelId <- function(){
  try(taQuery("DELETE FROM model_factory.model_overview WHERE model_id = 'test_model_id'"),silent = TRUE)
  taQuery("INSERT INTO model_factory.model_overview (model_id, model_description, score_id_type) VALUES ('test_model_id','Temporary model_id for test script, you can safly remove this','test')")
  deleteModelId('test_model_id')
  row <- taQuery("SELECT * FROM model_factory.model_overview WHERE model_id = 'test_model_id'")
  checkTrue(nrow(row) == 0,"deleteModelId did not delete the model")

}

test.deleteSession <- function(){
  session_id <<- "not_set"
  try(taQuery("DELETE FROM model_factory.model_overview WHERE model_id = 'test_model_id'"),silent = TRUE)
  taQuery("INSERT INTO model_factory.model_overview (model_id, model_description, score_id_type) VALUES ('test_model_id','Temporary model_id for test script, you can safly remove this','test')")

  try(sprintf("DELETE FROM model_factory.model_summary WHERE session_id in ('%s')",session_id),silent = TRUE)
  taQuery(sprintf("INSERT INTO model_factory.model_summary (session_id,variable) VALUES ('%s','col1')",session_id))

  try(sprintf("DELETE FROM model_factory.model_scores WHERE session_id in ('%s')",session_id),silent = TRUE)
  taQuery(sprintf("INSERT INTO model_factory.model_scores (session_id,id) VALUES ('%s','id_1')",session_id))

  try(sprintf("DELETE FROM model_factory.model_test_results WHERE session_id in ('%s')",session_id),silent = TRUE)
  taQuery(sprintf("INSERT INTO model_factory.model_test_results (session_id,score) VALUES ('%s',0.6)",session_id))

  try(sprintf("DELETE FROM model_factory.model_backtesting WHERE session_id in ('%s')",session_id),silent = TRUE)
  taQuery(sprintf("INSERT INTO model_factory.model_backtesting (session_id,predicted_value) VALUES ('%s',0.6)",session_id))

  try(sprintf("DELETE FROM model_factory.metadata_table WHERE session_id in ('%s')",session_id),silent = TRUE)
  taQuery(sprintf("INSERT INTO model_factory.metadata_table (session_id,session_key) VALUES ('%s','something you might want to know')",session_id))

  try(sprintf("DELETE FROM model_factory.run_history WHERE session_id in ('%s')",session_id),silent = TRUE)
  taQuery(sprintf("INSERT INTO model_factory.run_history (session_id) VALUES ('%s')",session_id))

  deleteSession(session_id)

  row <- taQuery("SELECT * FROM model_factory.model_overview WHERE model_id = 'test_model_id'")
  checkTrue(nrow(row) == 1,"deleteSession removed(or added) a model to model_overview")

  row <- taQuery(sprintf("SELECT * FROM model_factory.model_summary WHERE session_id = '%s'",session_id))
  checkTrue(nrow(row) == 0,"deleteSession did not remove session from model_summary")

  row <- taQuery(sprintf("SELECT * FROM model_factory.model_scores WHERE session_id = '%s'",session_id))
  checkTrue(nrow(row) == 0,"deleteSession did not remove session from model_scores")

  row <- taQuery(sprintf("SELECT * FROM model_factory.model_test_results WHERE session_id = '%s'",session_id))
  checkTrue(nrow(row) == 0,"deleteSession did not remove session from model_test_results")

  row <- taQuery(sprintf("SELECT * FROM model_factory.model_backtesting WHERE session_id = '%s'",session_id))
  checkTrue(nrow(row) == 0,"deleteSession did not remove session from model_backtesting")

  row <- taQuery(sprintf("SELECT * FROM model_factory.metadata_table WHERE session_id = '%s'",session_id))
  checkTrue(nrow(row) == 0,"deleteSession did not remove session from metadata_table")

  row <- taQuery(sprintf("SELECT * FROM model_factory.run_history WHERE session_id = '%s'",session_id))
  checkTrue(nrow(row) == 0,"deleteSession did not remove session from run_history")

  try(taQuery("DELETE FROM model_factory.model_overview WHERE model_id = 'test_model_id'"),silent = TRUE)
  try(taQuery(sprintf("DELETE FROM model_factory.model_summary WHERE session_id in ('%s')",session_id),silent = TRUE))
  try(taQuery(sprintf("DELETE FROM model_factory.model_scores WHERE session_id in ('%s')",session_id),silent = TRUE))
  try(taQuery(sprintf("DELETE FROM model_factory.model_test_results WHERE session_id in ('%s')",session_id),silent = TRUE))
  try(taQuery(sprintf("DELETE FROM model_factory.model_backtesting WHERE session_id in ('%s')",session_id),silent = TRUE))
  try(taQuery(sprintf("DELETE FROM model_factory.metadata_table WHERE session_id in ('%s')",session_id),silent = TRUE))
  try(taQuery(sprintf("DELETE FROM model_factory.run_history WHERE session_id in ('%s')",session_id),silent = TRUE))
}

test.getSummary <- function(){
  df <- data.frame(col1=c(1,2,3,4,5,6,7,8,9,10),col2=c(1,2,3,5,NA,5,7,8,9,10))
  s <- getSummary(df)
  df_sum <- data.frame(
                       mean = c(mean(df$col1, na.rm = TRUE),mean(df$col2, na.rm = TRUE)),
                       sd = c(sd(df$col1, na.rm = TRUE),sd(df$col2, na.rm = TRUE)),
                       median = c(median(df$col1, na.rm = TRUE),median(df$col2, na.rm = TRUE)),
                       min = c(min(df$col1, na.rm = TRUE),min(df$col2, na.rm = TRUE)),
                       max = c(max(df$col1, na.rm = TRUE),max(df$col2, na.rm = TRUE)),
                       n = c(length(df$col1),length(df$col2)),
                       n_na = c(sum(is.na(df$col1)),sum(is.na(df$col2))),
                       variable = as.character(names(df)),
                       stringsAsFactors = FALSE
  )

    checkTrue(all.equal(s,df_sum),sprintf("getSummary returned incorrect data %s",s))
}

test.storeSummary <- function(){
  session_id <<- 'not_set'
  df <- data.frame(col1=c(1,2,3,4,5,6,7,8,9,10),col2=c(1,2,3,5,NA,5,7,8,9,10))
  df_sum <- data.frame(
    mean = c(mean(df$col1, na.rm = TRUE),mean(df$col2, na.rm = TRUE)),
    sd = c(sd(df$col1, na.rm = TRUE),sd(df$col2, na.rm = TRUE)),
    median = c(median(df$col1, na.rm = TRUE),median(df$col2, na.rm = TRUE)),
    min = c(min(df$col1, na.rm = TRUE),min(df$col2, na.rm = TRUE)),
    max = c(max(df$col1, na.rm = TRUE),max(df$col2, na.rm = TRUE)),
    n = c(length(df$col1),length(df$col2)),
    n_na = c(sum(is.na(df$col1)),sum(is.na(df$col2))),
    variable = as.character(names(df)),
    stringsAsFactors = FALSE
  )
  try(taQuery(sprintf("DELETE FROM model_factory.model_summary WHERE session_id in ('%s')",session_id)),silent = TRUE)
  df_sum$session_id = c('not_set','not_set')
  storeSummary(df_sum)

  row <- taQuery(sprintf("SELECT session_id, mean,sd,median,\"min\",\"max\",n,\"n_na\", variable FROM model_factory.model_summary WHERE session_id = '%s' ORDER BY variable",session_id))
  checkTrue(nrow(row) == 2,"storeSummarydid stores more rows, or less, than what was exspected")
  checkTrue(all.equal(row,df_sum),sprintf("storeSummary did not store the data correctly  %s",row))

  try(taQuery(sprintf("DELETE FROM model_factory.model_summary WHERE session_id in ('%s')",session_id)),silent = TRUE)
}

test.pullSummary <- function(){
  df <- data.frame(col1=c(1,2,3,4,5,6,7,8,9,10),col2=c(1,2,3,5,NA,5,7,8,9,10))
  df_sum <- data.frame(
    session_id = c('not_set','not_set'),
    variable = as.character(names(df)),
    mean = c(mean(df$col1, na.rm = TRUE),mean(df$col2, na.rm = TRUE)),
    sd = c(sd(df$col1, na.rm = TRUE),sd(df$col2, na.rm = TRUE)),
    median = c(median(df$col1, na.rm = TRUE),median(df$col2, na.rm = TRUE)),
    min = c(min(df$col1, na.rm = TRUE),min(df$col2, na.rm = TRUE)),
    max = c(max(df$col1, na.rm = TRUE),max(df$col2, na.rm = TRUE)),
    n = c(length(df$col1),length(df$col2)),
    n_na = c(sum(is.na(df$col1)),sum(is.na(df$col2))),
    stringsAsFactors = FALSE
  )
  session_id <<- 'not_set'
  values <- c(length=2)
  for(i in 1:nrow(df_sum)) {
    row <- df_sum[i,3:8]
    values[i] = paste("( '",session_id,"','",df_sum$variable[i],"',",paste(row,collapse=" , "),",'",df_sum[i,8],"' )",sep="")
  }
  try(taQuery(sprintf("DELETE FROM model_factory.model_summary WHERE session_id in ('%s')",session_id)),silent = TRUE)
  taQuery(sprintf("INSERT INTO model_factory.model_summary (session_id, variable, mean, sd, median, min, max, n, n_na) VALUES %s",paste(values,collapse =",")))

  s <- pullSummary(session_id)
  s1 <- s[order(s$variable),]
  checkTrue(nrow(s1) == 2,sprintf("pullSummary did not retieve the right amount of rows %s",nrow(s1)))
  checkTrue(all.equal(s1,df_sum),sprintf("pullSummary did not retieve the data correctly  %s",s1))
  try(taQuery(sprintf("DELETE FROM model_factory.model_summary WHERE session_id in ('%s')",session_id)),silent = TRUE)

}

test.getTestResults <- function(){
  df <- data.frame(scores = c(0.9,0.87,0.86,0.86,0.85,0.85,0.84,0.84,0.83,0.82,0.81,0.8,0.79,0.77,0.76,0.74,0.72,0.7,0.65,0.6,0.55,0.5,0.45,0.4,0.35),
                 labels = c(1  ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0  ,1   ,1   ,0   ,0   ,1   ,1  ,0   ,0  ,0   ,1  ,1   ,0  ,0))
  df_r <- getTestResults(df$scores,df$labels)
}

test.getROC <- function(){
  df <- data.frame(scores = c(0.9,0.87,0.86,0.86,0.85,0.85,0.84,0.84,0.83,0.82,0.81,0.8,0.79,0.77,0.76,0.74,0.72,0.7,0.65,0.6,0.55,0.5,0.45,0.4,0.35),
                   labels = c(1  ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0  ,1   ,1   ,0   ,0   ,1   ,1  ,0   ,0  ,0   ,1  ,1   ,0  ,0))
  df_l <- getROC(df$scores,df$labels)
}

test.getLiftChart <- function(){
  df <- data.frame(scores = c(0.9,0.87,0.86,0.86,0.85,0.85,0.84,0.84,0.83,0.82,0.81,0.8,0.79,0.77,0.76,0.74,0.72,0.7,0.65,0.6,0.55,0.5,0.45,0.4,0.35),
                   labels = c(1  ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0  ,1   ,1   ,0   ,0   ,1   ,1  ,0   ,0  ,0   ,1  ,1   ,0  ,0))
  df_l <- getLiftChart(df$scores,df$labels)
}

test.getConfMatrix <- function(){
  df <- data.frame(scores = c(0.9,0.87,0.86,0.86,0.85,0.85,0.84,0.84,0.83,0.82,0.81,0.8,0.79,0.77,0.76,0.74,0.72,0.7,0.65,0.6,0.55,0.5,0.45,0.4,0.35),
                   labels = c(1  ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0  ,1   ,1   ,0   ,0   ,1   ,1  ,0   ,0  ,0   ,1  ,1   ,0  ,0))
  df_l <- getConfMatrix(df$scores,df$labels, 0.3, "probability")
  checkException(getConfMatrix(df$scores,df$labels,0.3 ))
  checkException(getConfMatrix(df$scores,df$labels,0.3, "not population" ))
}

test.getAccuracy <- function(){
  df <- data.frame(scores = c(0.9,0.87,0.86,0.86,0.85,0.85,0.84,0.84,0.83,0.82,0.81,0.8,0.79,0.77,0.76,0.74,0.72,0.7,0.65,0.6,0.55,0.5,0.45,0.4,0.35),
                   labels = c(1  ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0  ,1   ,1   ,0   ,0   ,1   ,1  ,0   ,0  ,0   ,1  ,1   ,0  ,0))
  df_l1 <- getAccuracy(df$scores,df$labels,0.815, "probability")
  df_l2 <- getAccuracy(df$scores,df$labels,0.3, "population" )
  checkException(getAccuracy(df$scores,df$labels,0.3, "not population" ))
}

test.storeTestResults <- function(){
  session_id <<- 'not_set'
  df <- data.frame(scores = c(0.9,0.87,0.86,0.86,0.85,0.85,0.84,0.84,0.83,0.82,0.81,0.8,0.79,0.77,0.76,0.74,0.72,0.7,0.65,0.6,0.55,0.5,0.45,0.4,0.35),
                   labels = c(1  ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0  ,1   ,1   ,0   ,0   ,1   ,1  ,0   ,0  ,0   ,1  ,1   ,0  ,0))
  df_r <- getTestResults(df$scores,df$labels)
  df_r <- df_r[order(df_r$scores, decreasing = TRUE),]
  rownames(df_r) <- NULL
  storeTestResults(df_r)
  df_sr <- ta.Query(sprintf("select score as scores, label as labels, population, target_population, true_positives, false_positives, true_negatives, false_negatives from model_factory.model_test_results where session_id = '%s'", session_id))
  df_sr <- df_sr[order(df_sr$scores, decreasing = TRUE),]
  rownames(df_sr) <- NULL
  checkTrue(all.equal(df_r,df_sr),sprintf("storeTestResults did not retieve the data correctly"))
}

test.pullTestResults <- function(){
  session_id <<- 'not_set'
  df <- data.frame(scores = c(0.9,0.87,0.86,0.86,0.85,0.85,0.84,0.84,0.83,0.82,0.81,0.8,0.79,0.77,0.76,0.74,0.72,0.7,0.65,0.6,0.55,0.5,0.45,0.4,0.35),
                   labels = c(1  ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0  ,1   ,1   ,0   ,0   ,1   ,1  ,0   ,0  ,0   ,1  ,1   ,0  ,0))
  df_r <- getTestResults(df$scores,df$labels)
  storeTestResults(df_r)
  df_pr <- pullTestResults(session_id)
  checkTrue(all.equal(df_r,df_pr),sprintf("pullTestResults did not retieve the data correctly"))
}

test.pullROC <- function(){
  session_id <<- 'not_set'
  df <- data.frame(scores = c(0.9,0.87,0.86,0.86,0.85,0.85,0.84,0.84,0.83,0.82,0.81,0.8,0.79,0.77,0.76,0.74,0.72,0.7,0.65,0.6,0.55,0.5,0.45,0.4,0.35),
                   labels = c(1  ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0  ,1   ,1   ,0   ,0   ,1   ,1  ,0   ,0  ,0   ,1  ,1   ,0  ,0))
  df_r <- getTestResults(df$scores,df$labels)
  storeTestResults(df_r)
  df_pr <- pullROC(session_id)
  df_gr <- getROC(df$scores,df$labels)
  checkTrue(all.equal(df_gr,df_pr),sprintf("pullROC and getROC did not give the same anser"))
}

test.pullLiftChart <- function(){
  session_id <<- 'not_set'
  df <- data.frame(scores = c(0.9,0.87,0.86,0.86,0.85,0.85,0.84,0.84,0.83,0.82,0.81,0.8,0.79,0.77,0.76,0.74,0.72,0.7,0.65,0.6,0.55,0.5,0.45,0.4,0.35),
                   labels = c(1  ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0  ,1   ,1   ,0   ,0   ,1   ,1  ,0   ,0  ,0   ,1  ,1   ,0  ,0))
  df_r <- getTestResults(df$scores,df$labels)
  storeTestResults(df_r)
  df_pr <- pullLiftChart(session_id)
  df_gr <- getLiftChart(df$scores,df$labels)
  checkTrue(all.equal(df_gr,df_pr),sprintf("pullLiftChart and getLiftChart did not give the same anser"))
}

test.pullConfMatrix <- function(){
  session_id <<- 'not_set'
  df <- data.frame(scores = c(0.9,0.87,0.86,0.86,0.85,0.85,0.84,0.84,0.83,0.82,0.81,0.8,0.79,0.77,0.76,0.74,0.72,0.7,0.65,0.6,0.55,0.5,0.45,0.4,0.35),
                   labels = c(1  ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0  ,1   ,1   ,0   ,0   ,1   ,1  ,0   ,0  ,0   ,1  ,1   ,0  ,0))
  df_r <- getTestResults(df$scores,df$labels)
  storeTestResults(df_r)
  df_pr <- pullConfMatrix(session_id, 0.3, "probability")
  df_gr <- getConfMatrix(df$scores,df$labels, 0.3, "probability")
  checkTrue(all.equal(df_gr,df_pr),sprintf("pullConfMatrix and getConfMatrix did not give the same anser for probability"))

  df_pr <- pullConfMatrix(session_id, 0.3, "population")
  df_gr <- getConfMatrix(df$scores,df$labels, 0.3, "population")
  checkTrue(all.equal(df_gr,df_pr),sprintf("pullConfMatrix and getConfMatrix did not give the same anser for population"))
}

test.pullAccuracy <- function(){
  session_id <<- 'not_set'
  df <- data.frame(scores = c(0.9,0.87,0.86,0.86,0.85,0.85,0.84,0.84,0.83,0.82,0.81,0.8,0.79,0.77,0.76,0.74,0.72,0.7,0.65,0.6,0.55,0.5,0.45,0.4,0.35),
                   labels = c(1  ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0   ,1   ,1   ,0  ,1   ,1   ,0   ,0   ,1   ,1  ,0   ,0  ,0   ,1  ,1   ,0  ,0))
  df_r <- getTestResults(df$scores,df$labels)
  storeTestResults(df_r)
  df_pr <- pullAccuracy(session_id, 0.3, "probability")
  df_gr <- getAccuracy(df$scores,df$labels, 0.3, "probability")
  checkTrue(all.equal(df_gr,df_pr),sprintf("pullAccuracy and getAccuracy did not give the same anser for probability"))

  df_pr <- pullAccuracy(session_id, 0.3, "population")
  df_gr <- getAccuracy(df$scores,df$labels, 0.3, "population")
  checkTrue(all.equal(df_gr,df_pr),sprintf("pullAccuracy and getAccuracy did not give the same anser for population"))
}

test.storeModelScores <- function(){


}
test.pullModelScores <- function(){

}

test.storeBackTestCoordinates <- function(){
  session_id <<- 'not_set'
  t <- c('2015-01-01','2015-01-02','2015-01-03','2015-01-04','2015-01-05')
  period <- as.POSIXlt(t,"Europe/Amsterdam")
  df <- data.frame("predicted_value"=c(1.5,2.3,3.1,4.9,5.7),"actual_value"=c(1.2,3.0,3.7,4.3,5.1), "period"=t)
  storeBackTestCoordinates(df$predicted_value,df$actual_value,df$period)
}

test.pullBackTestCoordinates <- function(){

}

test.getVarimp <- function(){

}

test.storeVarImp <- function(){

}

test.pullVarimp <- function(){

}

test.runOnAster <- function(){

}

.setup <- function(){
  session_id <<- 'not_set'
  try(taQuery("DELETE FROM model_factory.model_overview WHERE model_id='test_model_id'"),silent = TRUE)
  try(taQuery(sprintf("DELETE FROM model_factory.run_history WHERE session_id = '%s'",session_id)),silent = TRUE)
  try(taQuery("DELETE FROM model_factory.run_history WHERE session_id = 'not_set'"),silent = TRUE)

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
}

.tearDown <- function(){
  #try(a <- taQuery("DELETE FROM model_factory.model_overview WHERE model_id='test_model_id'"),silent = TRUE)
  #try(a <- taQuery(sprintf("DELETE FROM model_factory.run_history WHERE session_id = '%s'",session_id)),silent = TRUE)
  #try(a <- taQuery("DELETE FROM model_factory.run_history WHERE session_id = 'not_set'"),silent = TRUE)
  #taQuery("DROP TABLE IF EXISTS ")
}

