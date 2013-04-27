source("nhl-data.R")
rmr.options(backend= "local")

the.range <- 1:10  
the.year <- "2011-2012"

query.for <- function(q) {
  query <- get.query(q)
  function(k,v) {
    game.rdf <- game(v)
    sparql.rdf(game.rdf, query)
  }
}

matchups <- "select ?home ?away { ?game :hometeamname ?home . ?game :awayteamname ?away }"
actions <- "select ?game ?play ?type { ?game :play ?play . ?play a ?type }"

hdfs.data= to.dfs(get.data(the.range, the.year))
result <- mapreduce(
                    input= hdfs.data,
                    map= query.for(actions) )
ans <- from.dfs(result)

