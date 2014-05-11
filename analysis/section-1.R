source("definitions.R")
rmr.options(backend= "local")
#quartz()

# We're interested in the first 10 games of the 2011-2012 season
the.range <- 1:10
the.year <- "2011-2012"

# We want the number goals and penalties for each game
game.info.q <-
  conc("",
       "select ?goals ?pen {",
       " { select ?game (count(?x) as ?goals) ",
       "   { ?game :play ?x . ?x a :Goal } group by ?game } . ",
       " { select ?game (count(?x) as ?pen) ",
       "   { ?game :play ?x . ?x a :Penalty } group by ?game } ",
       "}")

# Each game is enhanced with an ontology and constructs (per definitions.R)
# and is queried with the game.info.q
map.job <- function(k,v) {
    enhanced.game <- get.enhanced(game(v))
    the.result <- sparql.rdf(enhanced.game, get.query(game.info.q))
    if (length(the.result) > 0) { the.result }
}

# We execute the map-reduce function (there's no reduce step here)
# and transform the resulting numeric matrix to a data.frame
run.it <- function() {
  hdfs.data <- to.dfs(get.data(the.range, the.year))
  result <- mapreduce(
                      input= hdfs.data,
                      map= map.job)
  r <- data.frame(from.dfs(result)$val)
  names(r) <- c("goals","penalties")
  r
}

# Outside of map-reduce, this is what we're doing with a sample game
# m <- get.sample.game(1, the.year)
# e <- get.enhanced(m)
# r <- sparql.rdf(e, get.query(game.info.q))

# Finally, a function to wrap up our visualization of the
# map-reduce results.  Is there a relationship between
# goals and penalties?
graph.it <- function(df) {
  attach(df)
  opar <- par(no.readonly=TRUE)

  par(lty=2, pch= 16)
  plot(goals, penalties)
  
  par(opar)
  detach(df)
}
