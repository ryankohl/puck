library("rrdf")

conc <- function(...) { paste( list(...), collapse="" ) }

ns.rdf <- function(x) { conc("http://www.w3.org/1999/02/22-rdf-syntax-ns#",x) }
ns.rdfs <- function(x) { conc("http://www.w3.org/2000/01/rdf-schema#",x) }
ns.nhl <- function(x) { conc("http://www.nhl.com/",x) }

rdf.blank <- function(x) { conc( "_:b", x ) }

is.nil <- function(x) { x == '' || is.null(x) }

good.fact <- function(x) { length(x) == 3 && !(TRUE %in% sapply(x, is.nil)) }

res.fact <- function(kb,s,p,o) {
  if (good.fact(c(s,p,o))) {
    add.triple(kb, subject=s, predicate=p, object=o)
  }
}
lit.fact <- function(kb,s,p,o,t) {
  if (good.fact(c(s,p,o))) {
    add.data.triple(kb, subject=s, predicate=p, data=as.character(o), type=t)
  }
}

