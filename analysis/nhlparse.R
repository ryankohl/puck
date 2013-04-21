library("rrdf")

conc <- function(...) {  paste( list(...), collapse="" ) }
ns.rdf <- function(x) { conc("http://www.w3.org/1999/02/22-rdf-syntax-ns#",x) }
ns.rdfs <- function(x) { conc("http://www.w3.org/2000/01/rdf-schema#",x) }
ns.nhl <- function(x) { conc("http://www.nhl.com/",x) }

get.query <- function(query) {
  one <- "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
  two <- "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
  three <- "prefix : <http://www.nhl.com/> "
  conc(one,two,three,query)
}

awayteamid   <- function(gid, x, kb) { lit.fact(kb, gid, ns.nhl("awayteamid"), x, "string") }
awayteamname <- function(gid, x, kb) { lit.fact(kb, gid, ns.nhl("awayteamname"), x, "string") }
awayteamnick <- function(gid, x, kb) { lit.fact(kb, gid, ns.nhl("awayteamnick"), x, "string") }
hometeamid   <- function(gid, x, kb) { lit.fact(kb, gid, ns.nhl("hometeamid"), x, "string") }
hometeamname <- function(gid, x, kb) { lit.fact(kb, gid, ns.nhl("hometeamname"), x, "string") }
hometeamnick <- function(gid, x, kb) { lit.fact(kb, gid, ns.nhl("hometeamnick"), x, "string") }

res.fact <- function(kb,s,p,o) { add.triple(kb, subject=s, predicate=p, object=o) }
lit.fact <- function(kb,s,p,o,t) { add.data.triple(kb, subject=s, predicate=p, data=as.character(o), type=t) }

parsed.fields <- c("awayteamid","awayteamname","awayteamnick",
                   "hometeamid","hometeamname","hometeamnick")

game.field <- function(gid, field, x, kb) {
  if (field %in% parsed.fields) { get(field)(gid, x[[field]], kb) }
}

game <- function(x) {
  kb <- new.rdf(ontology=FALSE)
  gid <- rdf.blank("game")
  res.fact(kb, gid, ns.rdf("type"), ns.nhl("Game"))
  game.data <- x[["data"]][["game"]]
  fields <- names(game.data)
  for (f in fields) { game.field(gid, f, game.data, kb) }
  kb
}
