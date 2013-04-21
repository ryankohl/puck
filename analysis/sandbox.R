source("nhlparse.R")
library(RJSONIO)
library(rmr2)
library(rrdf)
rmr.options(backend= "local")

get.datum <- function(n, dir, year) {
  the.file <- conc(dir,"/",year,"/","file-",n,".json")
  the.value <- fromJSON(the.file)
  the.key <- conc("game-",n)
  keyval(the.key, the.value)

}
get.data <- function() {
  the.range <- 1:10  
  the.dir <- "/Users/ryan/data/nhl"
  the.year <- "2011-2012"
  the.keyvals <- lapply(the.range, function(n) { get.datum(n, the.dir, the.year) } )  
  c.keyval(the.keyvals)
}

matchups <- function(k,v) { 
  q <- get.query("select ?home ?away { ?game :hometeamname ?home . ?game :awayteamname ?away }")
  sparql.rdf(game(v), q)
}

actions <- function(k,v) {
  q <- get.query("select ?game ?play ?type { ?game :play ?play . ?play a ?type }")
  sparql.rdf(game(v), q)
}

hdfs.data= to.dfs(get.data())
result <- mapreduce(
                    input= hdfs.data,
                    map= actions )
ans <- from.dfs(result)

