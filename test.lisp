(ql:quickload :info.read-eval-print.bson)

(defpackage :info.read-eval-print.bson.test
  (:use :cl :info.read-eval-print.bson))

(in-package :info.read-eval-print.bson.test)


;; {"hello": "world"}
(assert
 (bson= (bson "hello" "world")
        (decode (fast-io:octets-from '(#x16 #x00 #x00 #x00 #x02 104 101 108 108 111 #x00
                                       #x06 #x00 #x00 #x00 119 111 114 108 100 #x00 #x00)))))

;; {"BSON": ["awesome", 5.05, 1986]}
(assert
 (bson= (bson "BSON" '("awesome" 5.05d0 1986))
        (let ((*print-base* 16))
          (decode
           (fast-io:octets-from '(#x31 #x00 #x00 #x00 #x04 66 83 79 78 #x00
                                  #x26 #x00 #x00 #x00 #x02 48 #x00 #x08 #x00 #x00 #x00 97 119 101 115
                                  111 109 101 #x00 #x01 49 #x00 #x33 #x33 #x33 #x33 #x33 #x33 #x14
                                  #x40 #x10 50 #x00 #xc2 #x07 #x00 #x00 #x00 #x00))))))

(describe (decode
           (fast-io:octets-from '(#x31 #x00 #x00 #x00 #x04 66 83 79 78 #x00
                                  #x26 #x00 #x00 #x00 #x02 48 #x00 #x08 #x00 #x00 #x00 97 119 101 115
                                  111 109 101 #x00 #x01 49 #x00 #x33 #x33 #x33 #x33 #x33 #x33 #x14
                                  #x40 #x10 50 #x00 #xc2 #x07 #x00 #x00 #x00 #x00))))
;;→ #<BSON {100A86FFE3}>
;;     [standard-object]
;;   
;;   Slots with :INSTANCE allocation:
;;     HEAD  = (("BSON" "awesome" 5.05d0 1986))
;;     TAIL  = (("BSON" "awesome" 5.05d0 1986))
;;   
;;⇒ 


