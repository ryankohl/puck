source("nhl-data.R")
source("definitions.R")
rmr.options(backend= "local")

the.range <- 1:10  
the.year <- "2011-2012"

query.for <- function(q) {
  query <- get.query(q)
  function(k,v) {
    the.game <- game(v)
    sparql.rdf(the.game, query)
  }
}

matchups.q <- "select ?home ?away { ?game :hometeamname ?home . ?game :awayteamname ?away }"
actions.q <- "select ?game ?play ?type { ?game :play ?play . ?play a ?type }"

hdfs.data= to.dfs(get.data(the.range, the.year))
result <- mapreduce(
                    input= hdfs.data,
                    map= query.for(actions.q) )
ans <- from.dfs(result)


enhanced.query.for <- function(q) {
  the.query <- get.query(q)
  function(k,v) {
    raw.game <- game(v)
    enhanced.game <- get.enhanced(raw.game)
    the.result <- sparql.rdf(enhanced.game, the.query)
    if (length(the.result) > 0) { the.result }
  }
}

penalties.q <- get.query(conc("select ?game ?type ?desc ?pim {",
                            "?game :play ?p . ?p a :Penalty . ",
                            "?p a ?type . ",
                            "?p :desc ?desc . ",
                            "?type :penaltyMinutes ?pim } "))

hdfs.data2= to.dfs(get.data(the.range, the.year))
result2 <- mapreduce(
                    input= hdfs.data2,
                    map= enhanced.query.for(penalties.q) )
ans2 <- from.dfs(result2)

#the.file <- "../data/2010-2011/file-8.json"
#the.json <- fromJSON(the.file)
#the.game <- game(the.json)

#the.query <- get.query("select ?s ?p ?o { ?s a :Hooking . ?s ?p ?o }")
#the.result <- sparql.rdf(the.model, the.query)
