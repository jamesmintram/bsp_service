(def ring '[ring/ring-core "1.4.0" :exclusions [org.clojure/clojure]])

(defproject bspservice "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :plugins [[lein-ring "0.9.6"]
            ;[lein-beanstalk "0.2.7"]
            ]
  :ring {
    :handler bsp_service.core/handler
    :init bsp_service.core/test_boot
  }

  :aws {:beanstalk {:region "eu-west-1"
                    :s3-bucket "test-clojure-code-deploy"}}

  :dependencies [ [org.clojure/clojure "1.6.0"]
                  [org.clojure/core.async "0.1.346.0-17112a-alpha"]
  				        [compojure "1.4.0" :exclusions [ring/ring-core]]
                  [ring/ring-core "1.4.0" :exclusions [org.clojure/clojure]]
                  [amazonica "0.3.29" :exclusions [org.apache.httpcomponents/httpclient joda-time]]
                 ]

  ;:repl-options {:nrepl-middleware [lighttable.nrepl.handler/lighttable-ops]}

  :main ^:skip-aot bsp-service.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})



