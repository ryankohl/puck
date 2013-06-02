source("definitions.R")
rmr.options(backend= "local")

the.range <- 1:10
the.year <- "2011-2012"

team.info.q <-
  conc("select ?team ?goals ?assists ?penalties ",
       " { ?t a :Team . ?t :nick ?team",
       "   optional { ?t :numGoals ?goals } . ",
       "   optional { ?t :numAssists ?assists } . ",
       "   optional { ?t :numPenalties ?penalties }}")

do.work <- function(m) {
# let's take the input RDF model of the base JSON
# and execute the team.info.q
}

map.job <- function(k,v) {
  df <- data.frame(do.work(game(v)))
  teams <- data.frame(team= as.character(df$team), stringsAsFactors=FALSE)
  stats <- data.frame(lapply(df[,2:4], function(m) { as.numeric(as.character(m)) }))
  keyval(teams, stats)
}

reduce.job <- function(team, stats) {
  roll.up <- data.frame(rbind(colSums(stats)))
  p <- 2*roll.up$goals+roll.up$assists
  info <- data.frame(roll.up, points= p)
  keyval(team, info)
}

run.it <- function() {
  hdfs.data <- to.dfs(get.data(the.range, the.year))
  result <- mapreduce(
                      input= hdfs.data,
                      map= map.job,
                      reduce= reduce.job)
  r <- data.frame(from.dfs(result))
  names(r) <- c("team","goals","assists","penalties","points")
  r
}

graph.it <- function(df) {
  opar <- par(no.readonly=TRUE)

  x <- df[order(df$goals),]
  x$color[x$goals >= 250] <- "blue"
  x$color[x$goals >= 220 & x$goals < 250] <- "black"
  x$color[x$goals < 220] <- "red"
  dotchart(x$goals,
           labels=x$team,
           pch=19,
           gcolor="black",
           color= x$color
           )
  
  par(opar)
}
