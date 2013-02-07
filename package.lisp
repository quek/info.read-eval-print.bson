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
           #:+bson-max-key+

           #:+type-end-of-bson+
           #:+type-double+
           #:+type-string+
           #:+type-document+
           #:+type-array+
           #:+type-binary+
           #:+type-undefined+
           #:+type-object-id+
           #:+type-false+
           #:+type-true+
           #:+type-datetime+
           #:+type-null+
           #:+type-regex+
           #:+type-db-pointer+
           #:+type-java-script+
           #:+type-symbol+
           #:+type-code-w-s+
           #:+type-int32+
           #:+type-timestamp+
           #:+type-int64+
           #:+type-min-key+
           #:+type-max-key+))
