source("nhl-data.R")
source("definitions.R")
rmr.options(backend= "local")

the.range <- 1:10
the.year <- "2011-2012"

game.info.q <-
  conc("",
       "select ?goals ?pen {",
       " { select ?game (count(?x) as ?goals) ",
       "   { ?game :play ?x . ?x a :Goal } group by ?game } . ",
       " { select ?game (count(?x) as ?pen) ",
       "   { ?game :play ?x . ?x a :Penalty } group by ?game } ",
       "}")

enhanced.query.for <- function(q) {
  the.query <- get.query(q)
  function(k,v) {
    enhanced.game <- get.enhanced(game(v))
    the.result <- sparql.rdf(enhanced.game, the.query)
    if (length(the.result) > 0) { the.result }
  }
}

hdfs.data <- to.dfs(get.data(the.range, the.year))

result <- mapreduce(
  input= hdfs.data,
  map= enhanced.query.for(game.info.q))

ans <- data.frame(from.dfs(result)$val)

save(ans, file="GvsP.Rda")

get.sample <- function(num, year) {
  the.file <- conc("../data", "/",year,"/","file-",num,".json")
  the.json <- fromJSON(the.file)
  get.enhanced(game(the.json))
}

#sample.game <- get.sample(1, the.year)
