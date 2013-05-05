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

the.file <- "../data/2010-2011/file-8.json"
the.json <- fromJSON(the.file)
the.game <- game(the.json)
the.test <- get.query("select ?desc { ?x a :Penalty . ?x :desc ?desc }")
the.penalties <- sparql.rdf(the.game, the.test)

penaltyDef <- function(pen, match) {
  conc("prefix nhl: <http://www.nhl.com/> ",
       "construct { ?e a nhl:", pen, "} ",
       " { ?e a nhl:Penalty . ?e nhl:desc ?d . FILTER regex(?d, '.*(",
       match,
       ").*')}")
}
hooking <- penaltyDef("Hooking", "HOOKING")
the.model <- construct.rdf(the.game, hooking)
the.query <- get.query("select ?s ?p ?o { ?s a :Hooking . ?s ?p ?o }")
the.result <- sparql.rdf(the.model, the.query)

the.ontology <- load.rdf("../data/ontology/ontology.ttl", format="TURTLE")
penaltyMR <- function(k,v) {
  game.rdf <- game(v)
  q <- get.query(conc("select ?game ?type ?desc ?pim {",
                      "?game :play ?p . ?p a :Penalty . ",
                      "?p a ?type . ",
                      "?p :desc ?desc . ",
                      "?type :penaltyMinutes ?pim } "))
  penalties.rdf <- construct.rdf(game.rdf, hooking)
  enhanced1.rdf <- combine.rdf(game.rdf, penalties.rdf)
  enhanced.rdf <- combine.rdf(enhanced1.rdf, the.ontology)
  x <- sparql.rdf(enhanced.rdf, q)
  if (length(x) > 0) { x }
}
hdfs.data2= to.dfs(get.data(the.range, the.year))
result2 <- mapreduce(
                    input= hdfs.data2,
                    map= penaltyMR )
ans2 <- from.dfs(result2)
