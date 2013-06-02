source("definitions.R")
rmr.options(backend= "local")

the.range <- 1:10
the.year <- "2011-2012"

game.info.q <- {
# Let's get the number of Fights and Hits per game
  conc("select ?fights ?hits ",
       "{}")
}
map.job <- function(k,v) {
    enhanced.game <- get.enhanced(game(v))
    the.result <- sparql.rdf(enhanced.game, get.query(game.info.q))
    if (length(the.result) > 0) { the.result }
}

run.it <- function() {
  hdfs.data <- to.dfs(get.data(the.range, the.year))
  result <- mapreduce(
                      input= hdfs.data,
                      map= map.job)
  r <- data.frame(from.dfs(result)$val)
  names(r) <- c("fights","hits")
  r
}

graph.it <- function(df) {
  attach(df)
  opar <- par(no.readonly=TRUE)

  par(lty=2, pch= 16)
  plot(fights, hits)
  
  par(opar)
  detach(df)
}

