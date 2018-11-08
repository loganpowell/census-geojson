(ns census.statsAPI.core
  (:require
    [cuerdas.core :as s]
    [cljs.core.async :as <|]
    [census.utils.core :as ut]
    [census.test.core :as ts]
    [census.wmsAPI.core :as wms]))


(defn kv-pair->str [[k v] separator]
  (s/join separator [(name k) (str v)]))

(defn stats-url-builder
  "Composes a URL to call Census' statistics API"
  [{:keys [vintage sourcePath geoHierarchy values predicates statsKey]}]
  (str ut/base-url-stats
       (str vintage)
       (s/join (map #(str "/" %) sourcePath))
       "?get="
       (if (some? values)
           (s/join "," values)
           "")
       (if (some? predicates)
           (str "&" (str (s/join "&" (map #(kv-pair->str % "=") predicates))))
           "")
       (ut/keys->strs
         (if (= 1 (count geoHierarchy))
             (str "&for=" (kv-pair->str (first geoHierarchy) ":"))
             (str "&in="  (s/join "%20" (map #(kv-pair->str % ":") (butlast geoHierarchy)))
                  "&for=" (kv-pair->str (last geoHierarchy) ":"))))
       "&key=" statsKey))



; ~~~888~~~                                        888
;    888    888-~\   /~~~8e  888-~88e  d88~\  e88~\888 888  888  e88~~\  e88~~8e  888-~\  d88~\
;    888    888          88b 888  888 C888   d888  888 888  888 d888    d888  88b 888    C888
;    888    888     e88~-888 888  888  Y88b  8888  888 888  888 8888    8888__888 888     Y88b
;    888    888    C888  888 888  888   888D Y888  888 888  888 Y888    Y888    , 888      888D
;    888    888     "88_-888 888  888 \_88P   "88_/888 "88_-888  "88__/  '88___/  888    \_88P


(defn parse-if-number
  [s]
  (if (s/numeric? s)
    (s/parse-number s)
    s))

(defn xf!-csv-response->JSON
  "
  Stateful transducer, which stores the first item as a list of a keys to apply
  (via `zipmap`) to the rest of the items in a collection. Serves to turn the
  Census API response into a more conventional JSON format.

  If provided `:keywords` as an argument, will return a map with Clojure keys.
  Otherwise, will return map keys as strings.
  "
  ([{:keys [values predicates]}] (xf!-csv-response->JSON [{:keys [values predicates]} nil]))
  ([{:keys [values predicates]} keywords?]
   (let [parse-range [0 (+ (count values) (count predicates))]]
     (ut/xf!<<
       (fn [state rf result input]
         (let [prev @state]
           (if (nil? prev)
               (if (= keywords? :keywords)
                   (do (vreset! state (mapv ut/strs->keys input)) nil)
                   (do (vreset! state input) nil))
               (if (= keywords? :keywords)
                   (rf result
                       (zipmap (vec (map keyword @state))
                               (ut/map-idcs-range parse-if-number
                                               parse-range
                                               input)))
                   (rf result
                       (zipmap @state
                               (ut/map-idcs-range parse-if-number
                                               parse-range
                                               input)))))))))))


(defn xfxf!-e?->csv->JSON
  [args keywords?]
  (comp
    (map ut/throw-err)
    (ut/xfxf<< (xf!-csv-response->JSON args keywords?) conj)))


(defn IO-pp->census-stats
  "
  Internal function for calling the Census API using a Clojure Map and getting
  stats returned as a clojure map.

  Note: Inside `go` blocks, any map literals `{}` are converted into hash-maps.
  Make sure to bind the args in a wrapping `(let [args ...(go` rather than from
  within a shared `go` context.
  "
  [=I= =O=]
  (<|/go (let [args  (<|/<! =I=)
               url   (stats-url-builder args)
               =url= (<|/chan 1)
               =res= (<|/chan 1 (xfxf!-e?->csv->JSON args :keywords))]
           (prn url)
           (<|/>! =url= url)
           ; IO-ajax-GET closes the =res= chan; pipeline-async closes the =url= when =res= is closed
           (<|/pipeline-async 1 =res= (ut/I=O<<=IO= ut/IO-ajax-GET-json) =url=)
           ; =O= chan is closed by the consumer; pipeline closes the =res= when =O= is closed
           (<|/>! =O= (<|/<! =res=))
           (<|/close! =url=)
           (<|/close! =res=))))



(defn getCensusStats
  "
  Library function, which takes a JSON object as input, constructs a call to the
  Census API and returns the data as _standard JSON_ (rather than the default
  csv-like format from the naked API).
  "
  ([args cb] (getCensusStats args cb nil))
  ([args cb keywords?]
   (if (= keywords? :keywords)
     ((wms/Icb<-wms-args<<=IO= IO-pp->census-stats) args cb)
     ((wms/Icb<-wms-args<<=IO= IO-pp->census-stats) args #(cb (js/JSON.stringify (clj->js %)))))))
