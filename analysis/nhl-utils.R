library("rrdf")

# take n-many strings and push 'em all together into a single string
# ie. we're concatenating
conc <- function(...) { paste( list(...), collapse="" ) }

# namespaces we'll use in the project
ns.rdf <- function(x) { conc("http://www.w3.org/1999/02/22-rdf-syntax-ns#",x) }
ns.rdfs <- function(x) { conc("http://www.w3.org/2000/01/rdf-schema#",x) }
ns.nhl <- function(x) { conc("http://www.nhl.com/",x) }

# return a blank node version of the input value
rdf.blank <- function(x) { conc( "_:b", x ) }

# a value is nil if it's an empty string or null
is.nil <- function(x) { x == '' || is.null(x) }

# good facts have exactly three parts (subj, pred, obj) and none of 'em are nil
good.fact <- function(x) { length(x) == 3 && !(TRUE %in% sapply(x, is.nil)) }

# add a resouce fact to a kb, as long as it's a good fact
res.fact <- function(kb,s,p,o) {
  if (good.fact(c(s,p,o))) {
    add.triple(kb, subject=s, predicate=p, object=o)
  }
}

# add a literal fact to a kb, as long as it's a good fact
lit.fact <- function(kb,s,p,o,t) {
  if (good.fact(c(s,p,o))) {
    add.data.triple(kb, subject=s, predicate=p, data=as.character(o), type=t)
  }
}

