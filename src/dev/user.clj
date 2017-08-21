(ns user
  (:require
    [fulcro-spec.selectors :as sel]
    [fulcro-spec.suite :as suite]))

(suite/def-test-suite my-test-suite
  {:config       {:port 8888}
   :test-paths   ["src/test"]
   :source-paths ["src/main"]}
  {:available #{:focused}
   :default   #{::sel/none :focused}})
