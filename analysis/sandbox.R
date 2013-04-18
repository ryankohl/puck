conc <- function(...) {  paste( list(...), collapse="" ) }
xsd.suffix <- function(x) { conc( "^^<http://www.w3.org/2001/XMLSchema#", x, ">" ) }

rdf.node <- function(n,x) { conc( "<", n, x, ">" ) }
rdf.blank <- function(x) { conc( "_:b", x ) }
literal <- function(x,type) { conc( "'", x, "'", xsd.suffix(type) ) }

rdf.str <- function(x) { literal(x, "string")  }
rdf.int <- function(x) { literal(x, "integer") }
rdf.date <- function(x) { literal(x, "date")  }
rdf.datetime <- function(x) { literal(x, "datetime") }
fact <- function(x,y,z) {
  if (x != '' && y != '' && z != '') { paste( list(x,y,z), collapse=" ") }
}


rdf <- "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
rdfs <- "http://www.w3.org/2000/01/rdf-schema#"
nhl <- "http://www.nhl.com/"

awayteamid   <- function(gid, x) { fact( gid, rdf.node(nhl,"awayteamid"), rdf.str(x) ) }
awayteamname <- function(gid, x) { fact( gid, rdf.node(nhl,"awayteamname"), rdf.str(x) ) }
awayteamnick <- function(gid, x) { fact( gid, rdf.node(nhl,"awayteamnick"), rdf.str(x) ) }
hometeamid   <- function(gid, x) { fact( gid, rdf.node(nhl,"hometeamid"), rdf.str(x) ) }
hometeamname <- function(gid, x) { fact( gid, rdf.node(nhl,"hometeamname"), rdf.str(x) ) }
hometeamnick <- function(gid, x) { fact( gid, rdf.node(nhl,"hometeamnick"), rdf.str(x) ) }

game.field <- function(gid, field, x) {
  switch(field,
         "awayteamid" = awayteamid(gid, x[[field]]),
         "awayteamname" = awayteamname(gid, x[[field]]),
         "awayteamnick" = awayteamnick(gid, x[[field]]),
         "hometeamid" = hometeamid(gid, x[[field]]),
         "hometeamname" = hometeamname(gid, x[[field]]),
         "hometeamnick" = hometeamnick(gid, x[[field]])
)}

game <- function(x) {
  facts <- c()
  gid <- rdf.blank("game")
  facts <- append(facts, fact(gid, rdf.node(rdf,"type"), rdf.node(nhl,"Game")))
  game.data <- x[["data"]][["game"]]
  fields <- names(game.data)
  facts <- append(facts, sapply(fields, function(f) { game.field(gid, f, game.data) }))
  paste(facts, collapse=" . \n ")
}

library(RJSONIO)
library(rmr2)
rmr.options(backend= "local")
j <- fromJSON("/Users/ryan/src/r/winston/data/file-1.json")
x2 <- readLines("/Users/ryan/data/nhl/2011-2012/file-2.json")
game <- j$data$game
summary(game)
summary(game$plays)

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


hdfs.data= to.dfs(get.data())
result <- mapreduce(
                    input= hdfs.data,
                    map= function(k,v) { game(v) })
ans <- from.dfs(result)

ans$val[2]
