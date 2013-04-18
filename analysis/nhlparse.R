source("rdfparse.R")
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
