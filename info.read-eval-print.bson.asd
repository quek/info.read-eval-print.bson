;;;; info.read-eval-print.bson.asd

(asdf:defsystem #:info.read-eval-print.bson
  :serial t
  :description "Describe info.read-eval-print.bson here"
  :author "TAHARA Yoshinori <read.eval.print@gmail.com"
  :license "BSD Licence"
  :components ((:file "package")
               (:file "bson")
               (:file "reader"))
  :depends-on (:fast-io
               :babel
               :ieee-floats
               :local-time
               :cl-ppcre
               :md5
               :bordeaux-threads
               :iolib))

