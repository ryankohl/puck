puck
====

This is an analysis project that uses semantic technologies
to help transform a dataset into a useful shape.

Clojure is used in the project to download the play-by-play
dataset published by the National Hockey League.

R is used for all data analysis.  In particular, the rmr2 library
from Revolution Analytics is used to perform map-reduce operations 
and the rrdf library is used to incorporate semantic processing 
into the analytic process.

In the R environment, the following packages will need to be installed:

```R
install.packages(pkgs=c("rJava","rmr2","rrdflibs","rrdf","RJSONIO"))
```

The rmr2 package is a bit tricky to install - you'll want to 
follow the instructions [here](https://github.com/RevolutionAnalytics/RHadoop/wiki/rmr).