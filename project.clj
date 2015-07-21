(defproject bsp_service "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}

  :plugins [[lein-ring "0.8.11"]]
  :ring {:handler bsp_service.core/handler}

  :dependencies [[org.clojure/clojure "1.6.0"]
  				[liberator "0.13"]
  				[compojure "1.3.4"]
                [ring/ring-core "1.2.1"]]

  :main ^:skip-aot bsp-service.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
