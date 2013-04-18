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
