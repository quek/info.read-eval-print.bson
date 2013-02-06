;;;; package.lisp

(defpackage #:info.read-eval-print.bson
  (:use #:cl)
  (:export #:encode #:decode #:bson #:bson= #:value
           #:regex
           #:javascript-code
           #:code-wit-scope
           #:+bson-true+
           #:+bson-false+
           #:+bson-undefined+
           #:+bson-null+
           #:+bson-empty-array+
           #:+bson-min-key+
           #:+bson-max-key+))

