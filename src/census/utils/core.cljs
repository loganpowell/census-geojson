(ns census.utils.core
  (:require
    [cljs.core.async :as <|]
    [ajax.core :refer [GET POST]]
    [cljs-promises.async :refer [pair-port] :refer-macros [<?]]
    [cuerdas.core :as s]
    [com.rpl.specter :as sp]
    [cljs.pprint :refer [pprint]]
    [linked.core :as linked]
    [oops.core :as ob]
    [cljs.reader :as r]
    ["fs" :as fs]
    ["dotenv" :as env]))


(def stats-key (ob/oget (env/load) ["parsed" "Census_Key_Pro"]))

(def $geoKeyMap$ (atom {}))

(def base-url-stats "https://api.census.gov/data/")

(def base-url-wms "https://tigerweb.geo.census.gov/arcgis/rest/services/")

(def base-url-geojson "https://raw.githubusercontent.com/loganpowell/census-geojson/master/GeoJSON")

(def base-url-geoKeyMap "https://raw.githubusercontent.com/loganpowell/census-geojson/master/src/census/geojson/index.edn")

(def base-url-database "...")

(defn read-edn [path] (r/read-string (str (fs/readFileSync path))))

(def vec-type cljs.core/PersistentVector)

(def amap-type cljs.core/PersistentArrayMap)

(def err-type js/Error)

(defn error [e] (js/Error. e))

(def MAP-NODES
  "From [specter's help page](https://github.com/nathanmarz/specter/wiki/Using-Specter-Recursively#recursively-navigate-to-every-map-in-a-map-of-maps)"
  (sp/recursive-path [] p (sp/if-path map? (sp/continue-then-stay sp/MAP-VALS p))))

(defn deep-reverse-map
  "Recursively reverses the order of the key/value _pairs_ inside a map"
  {:test
   #(assert (= (deep-reverse-map {:i 7 :c {:e {:h 6 :g 5 :f 4} :d 3} :a {:b 2}})
               {:a {:b 2} :c {:d 3 :e {:f 4 :g 5 :h 6}} :i 7}))}
  [m]
  (sp/transform MAP-NODES
               #(into {} (reverse %))
               m))

(test deep-reverse-map)

(defn deep-linked-map
  "
  Recursively converts any map into a `linked` map (preserves insertion order)
  TODO - Testing:
  [core.async](https://github.com/clojure/core.async/blob/master/src/test/cljs/cljs/core/async/tests.cljs)
  "
  [m]
  (sp/transform MAP-NODES #(into (linked/map) (vec %)) m))

(defn map-rename-keys
  "
  Applies a function over the keys in a provided map
  "
  [f m]
  (sp/transform sp/MAP-KEYS f m))


(defn map-over-keys
  "
  Applies a function to all values of a provided map
  "
  [f m]
  (sp/transform sp/MAP-VALS f m))

(defn keys->strs
  "
  Translates Clojure (edn) key-forms of geographic identifyers into strings,
  which are valid as parameters of a Census Data API URL construction.
  "
  {:test #(assert
            (= (keys->strs
                 (name :american-indian-area!alaska-native-area-_reservation-or-statistical-entity-only_-_or-part_))
               "american indian area/alaska native area (reservation or statistical entity only) (or part)"))}
  [s]
  (s/replace s #"-_|_|!|-"
             {"-_" " (" "_" ")" "!" "/" "-" " "}))


(defn strs->keys
  "
  Translates strings valid as parameters of a Census Data API URL construction
  to Clojure (edn) key-forms of geographic identifyers. Also valid URL components
  of the raw.github directory structure.
  "
  {:test #(assert
            (= (keyword
                 (strs->keys "american indian area/alaska native area (reservation or statistical entity only) (or part)"))
               :american-indian-area!alaska-native-area-_reservation-or-statistical-entity-only_-_or-part_))}
  [s]
  (s/replace s #" \(|\)|/| "
             {" (" "-_" ")" "_" "/" "!" " " "-"}))


(defn IO-ajax-GET-json
  "
  I/O (chans) API which takes a URL from an input port (=I=), makes a `cljs-ajax`
  GET request to the provided URL and puts the response in the output (=O=) port.
  "
  [=URL= =RES=]
  (let [args {:response-format :json
              :handler
              (fn [r] (<|/go (<|/>! =RES= r) (<|/close! =RES=)))
              :error-handler
              (fn [e] (<|/go (<|/>! =RES= (error (get-in e [:parse-error :original-text]))) (<|/close! =RES=)))
              :keywords?       true}]
    (<|/go (GET (<|/<! =URL=) args))))


(defn IO-cache-GET-edn
  "
  Takes an atom as a one-time cachable value and returns a function that accepts
  I/O ports as with IO-ajax-GET.  On first remote GET, the atom is hydrated via
  an actual HTTP request. On following calls, the atom is dereferenced to provide
  the response. Only useful for a single value that doesn't need to change during
  a runtime.
  "
  [cache]
  (fn [=URL= =RES=]
      (if (empty? @cache)
          (let [args {:handler
                      (fn [r] (<|/go (<|/>! =RES= (r/read-string r))
                                     (reset! cache (r/read-string r))
                                     (<|/close! =RES=)))
                      :error-handler
                      (fn [e] (<|/go (<|/>! =RES= (error (get-in e [:parse-error :original-text])))
                                     (<|/close! =RES=)))}]
               (<|/go (GET (<|/<! =URL=) args)))
          (<|/go (<|/>! =RES= @cache) (<|/close! =RES=)))))


(defn js->args
  [args]
  (if (= (type args) amap-type)
    (let [{:keys [vintage]} args]
      (sp/setval :vintage (str vintage) args))
    (let [geoCljs (js->clj (ob/oget args "geoHierarchy"))
          vintage (ob/oget args "vintage")
          geoKeys (map-rename-keys strs->keys geoCljs)]
      (do (ob/oset! args "vintage"      (clj->js (str vintage)))
          (ob/oset! args "geoHierarchy" (clj->js geoKeys))
          (js->clj args :keywordize-keys true)))))

(defn args->js
  [{:keys [geoHierarchy] :as args}]
  (let [geoKeys (map-rename-keys #(keys->strs (name %)) geoHierarchy)]
    (prn (clj->js geoKeys))
    (clj->js (sp/setval :geoHierarchy geoKeys args))))


(defn throw-err
  "
  Throws an error... meant to be used in transducer `comp`osed with another
  transducer or as `(map u/throw-error)`.
  "
  [x]
  (if (instance? err-type x)
    (throw x)
    x))

(defn I=O<<=IO=
  "
  Adapter, which wraps asynchronous I/O ports input to provide a synchronous
  input.

  This is good for kicking off async functions, but also is the required
  signature/contract for `pipeline-async`.
  "
  [f]                            ; takes an async I/O function
  (fn [I =O=]                    ; returns a function with a sync input / `chan` output
    (let [=I= (<|/chan 1)]       ; create internal `chan`
      (<|/go (<|/>! =I= I)       ; put sync `I` into `=I=`
             (f =I= =O=)         ; call the wrapped function with the newly created `=I=`
             (<|/close! =I=))))) ; close the port to flush out values


(defn args+cb<<=IO=
  "
  Adapter, which wraps asynchronous I/O ports input to provide a synchronous
  input and expose the output to a callback and converts any #js args to proper
  cljs syntax (with keyword translation)

  This is good for touch & go asynchronous functions, which do not require
  'enduring relationships' or concerted application between other async
  functions (e.g., exposing asynchronous functions as a library).
  "
  [f]                                           ; takes an async I/O function
  (fn [I cb ?state]                             ; returns a function with sync input  / callback for output
    (let [=I=  (<|/chan 1)                      ; create two internal `chan`s for i/o
          =O=  (<|/chan 1 (map throw-err))
          args (js->args I)]                    ; converts any #js types to cljs with proper keys
      (<|/go (<|/>! =I= args)
             (f =I= =O= ?state)                 ; apply the async I/O function with the internal `chan`s
             (<|/take! =O= #(do (cb %)          ; use async `take!` to allow lambdas/closures
                                (<|/close! =I=) ; close the ports to flush the values
                                (<|/close! =O=)))))))


(defn xf<<
  "
  Transducifier wrapper, which takes the seed of a transducer (essential
  operation) with a standardized `xf result input` contract and wraps it in the
  necessary boilerplate to correctly function as a stateless transducer.

  Example of tranducer seed with contract required for this wrapper:

  (defn xf-seed-form
    [xf result input]
    (xf result {(keyword (get-in input [:properties :GEOID])) input}))
  "
  [f]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input] (f rf result input)))))


(defn xf!<<
  "
  Stateful transducifier wrapper, which takes the seed of a transducer (essential
  operation) with a standardized `xf result input` contract and wraps it in the
  necessary boilerplate to correctly function as a _stateful_ transducer.

  Only avails a single state container: `state`

  Example of tranducer seed with contract required for this wrapper:

  (defn xf!-seed-form
    [state xf result input]
      (let [prev @state]
        (if (nil? prev)
            (do (vreset! state (vec (map keyword item)))
              nil)
            (xf result (zipmap prev (vec item))))))
  "
  [f]
  (fn [rf]
    (let [state (volatile! nil)]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result input] (f state rf result input))))))



(defn xfxf<<
  "
  Transducer, which wraps a transducer to provide the right level of contract
  for a core.async chan through which data is not an item, but a collection.
  I.e., treating the collection as a single transducible item.
  "
  [xfn rf-]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result item]
       (rf result (transduce xfn rf- item))))))


(defn map-target
  "
  Maps a provided function to a specific index + 1 of a provided collection.
  "
  [f target coll]
  (map-indexed
    #(if (zero? (mod (inc %1) target)) (f %2) %2)
    coll))


(defn map-target-idcs
  "
  Maps a provided function over a given vector of indeces of a provided
  collection.
  "
  [f targets coll]
  (sp/transform [sp/INDEXED-VALS (sp/selected? sp/FIRST (set targets)) sp/LAST] f coll))


(defn map-idcs-range
  "
  Maps a provided function over a given range of indeces (vector of beginning
  to end) of a provided collection.
  "
  [f [r-start r-end] coll]
  (sp/transform [sp/INDEXED-VALS (sp/selected? sp/FIRST (set (range r-start r-end))) sp/LAST] f coll))


