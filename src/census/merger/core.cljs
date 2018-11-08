(ns census.merger.core
  (:require
    [cljs.core.async :as <|]
    [ajax.core :refer [GET POST]]
    [cljs.pprint :refer [pprint]]
    [clojure.repl :refer [source]]
    [census.test.core :as ts]
    [census.utils.core :as ut :refer [stats-key $geoKeyMap$]]
    [census.geoAPI.core :refer [IO-pp->census-GeoJSON]]
    [census.statsAPI.core :refer [IO-pp->census-stats]]
    [census.geojson.core :refer [geo+config->mkdirp->fsW!]]))

(defn xf<-stats+geoids
  "
  Higher-order transducer (to enable transduction of a list conveyed via `chan`).
  Takes an integer argument denoting the number of stat vars the user requested.
  The transducer is used to transform each item from the Census API response
  collection into a new map with a hierarchy that will enable deep-merging of
  the stats with a GeoJSON `feature`s `:properties` map.
  "
  [vars#]
  (ut/xf<< (fn [rf result input]
             (rf result {(keyword (reduce str (vals (take-last (- (count input) vars#) input))))
                         {:properties input}}))))

(defn get-geoid?s
  "
  Takes the request argument and pulls out a vector of the component identifiers
  from the geoKeyMap, which is used to construct the UID for the GeoJSON. Used
  in deep-merging with statistics.
  "
  [geoK {:keys [geoHierarchy vintage]}]
  (let [[& ids] (get-in geoK [(key (last geoHierarchy)) (keyword vintage) :id<-json])]
    ids))

(defn geoid<-feature
  "
  Takes the component ids from with the GeoJSON and a single feature to
  generate a :GEOID if not available within the GeoJSON.
  "
  [ids m]
  (keyword (reduce str (map #(get-in m [:properties %]) ids))))


(defn xf<-features+geoids
  "
  A function, which returns a transducer after being passed a. The transducer is used to
  transform each item withing a GeoJSON FeatureCollection into a new map with a
  hierarchy that will enable deep-merging of the stats with a stat map.
  "
  [ids]
  (ut/xf<<
    (fn [rf result input]
      (rf result {(geoid<-feature ids input) input})))) ;


(defn deep-merge-with
  "
  Recursively merges two maps together along matching key paths. Implements
  `clojure/core.merge-with`.

  [stolen](https://gist.github.com/danielpcox/c70a8aa2c36766200a95#gistcomment-2711849)
  "
  [& maps]
  (apply merge-with
         (fn [& args]
           (if (every? map? args)
             (apply deep-merge-with args)
             (last args)))
         maps))


(defn xf<-merged->filter
  "
  Transducer, which takes 2->3 keys that serve to filter a merged list of two
  maps to return a function, which returns a list of only those maps which have
  a key from both maps. The presence of both keys within the map signifies that
  the maps have merged. This ensures the returned list contains only the overlap
  between the two, i.e., excluding non-merged maps.
  "
  [s-key1 s-key2 g-key]
  (ut/xf<<
    (fn [rf result item]
     (let [[_ v] (first item)]
       (if (or (and (nil? (get-in v [:properties s-key1]))
                    (nil? (get-in v [:properties s-key2])))
               (nil? (get-in v [:properties g-key])))
         (rf result)
         (rf result v))))))


(defn merge-geo+stats
  "
  Higher Order Function to be used in concert with `core.async/map`, which takes
  three Clojure keywords and returns a function, which takes two
  input maps and deep-merges them into one based on common keys,
  and then filters them to return only those map-items that contain an
  identifying key from each source map. Used to remove unmerged items.
  "
  [s-key1 s-key2 g-key]
  (fn [stats-coll geo-coll]
    (->>
      (for [[_ pairs] (group-by keys (concat stats-coll geo-coll))]
        (apply deep-merge-with pairs))
      (transduce (xf<-merged->filter s-key1 s-key2 g-key) conj))))

(defn xfxf-e?->features+geoids
  [ids]
  (comp
    (map ut/throw-err)
    (map #(get % :features))
    (ut/xfxf<< (xf<-features+geoids ids) conj)))

(defn xfxf-e?->stats+geoids
  [vars#]
  (comp
    (map ut/throw-err)
    (ut/xfxf<< (xf<-stats+geoids vars#) conj)))


(defn IO-geo+stats
  "
  Takes an arg map to configure a call the Census' statistics API as well as a
  matching GeoJSON file. The match is based on `vintage` and `geoHierarchy` of
  the arg map. The calls are spun up (simultaneously) into parallel `core.async`
  processes for speed. Both calls return their results via a `core.async`
  channel (`chan`) - for later census.merger - via `put!`. The results from the Census
  stats `chan` are passed into a local `chan` to store the state.  A
  `deep-merge` into the local `chan` combines the stats results with the GeoJSON
  values. Note that the GeoJSON results can be a superset of the Census stats'
  results. Thus, superfluous GeoJSON values are filtered out via a `remove`
  operation on the collection in the local `chan`.
  "
  [=I= =O=]
  (let [=geo= (<|/chan 1)]
    ((ut/I=O<<=IO= (ut/IO-cache-GET-edn $geoKeyMap$)) ut/base-url-geoKeyMap =geo=)
    (<|/go (let [I          (<|/<! =I=)
                 geoK       (<|/<! =geo=)
                 args       (ut/js->args I)
                 ids        (get-geoid?s geoK args)
                 vars#      (+ (count (get args :values))
                               (count (get args :predicates)))
                 s-key1     (keyword (first (get args :values)))
                 s-key2     (first (keys (get args :predicates)))
                 g-key      (first ids)
                 =args=     (<|/promise-chan)
                 =stats=    (<|/chan 1 (xfxf-e?->stats+geoids vars#))
                 =features= (<|/chan 1 (xfxf-e?->features+geoids ids))
                 =merged=   (<|/map  (merge-geo+stats s-key1 s-key2 g-key)
                                     [=stats= =features=]
                                     1)]
             (<|/>! =args= args)
             (IO-pp->census-stats =args= =stats=)
             (IO-pp->census-GeoJSON =args= =features=)
             (<|/>! =O= {:type "FeatureCollection"
                         :features (<|/<! =merged=)})
             (<|/close! =args=)
             (prn "working on it....")))))
