(ns download
  (:require [http.async.client :as http]))

(defn head [y] (str "http://live.nhl.com/GameData/" y (+ y 1)"/" y "02"))
(def tail "/PlayByPlay.json")

(defn get-my-number[i]
  (cond 
   (< i 10) (str "000" i)
   (< i 100) (str "00" i)
   (< i 1000) (str "0" i)
   :else (str i)	))

(defn get-my-file [i y]
  (let [inp (slurp (str (head y) (get-my-number i) tail))
        filename (str "data/file-" i ".json")]
    (Thread/sleep 200)
    (prn filename)
    (spit filename inp)))

(defn download-data [a b y]  (doseq [i (range a (+ b 1))] (get-my-file i y)))

(comment 
"download the first 20 games for the 2010 season"
(download-data 1 20 2010)
)