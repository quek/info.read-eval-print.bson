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

(decode
 (fast-io:octets-from '(#x31 #x00 #x00 #x00 #x04 66 83 79 78 #x00
                        #x26 #x00 #x00 #x00 #x02 48 #x00 #x08 #x00 #x00 #x00 97 119 101 115
                        111 109 101 #x00 #x01 49 #x00 #x33 #x33 #x33 #x33 #x33 #x33 #x14
                        #x40 #x10 50 #x00 #xc2 #x07 #x00 #x00 #x00 #x00)))
;;⇒ {"BSON": ["awesome", 5.05d0, 1986]}


(assert (string= "{\"a\": \"b\"}"
                 (princ-to-string (bson "a" "b"))))
(assert (string= "{\"a\": {\"a\": \"b\"}}"
                 (princ-to-string (bson "a" (bson "a" "b")))))

(assert (local-time:timestamp=
         (datetime 2013 1 2 3 4 5 6)
         (value (decode (encode (bson :foo (datetime 2013 1 2 3 4 5 6 7 8)))) :foo)))

(assert (bson=
         (bson :_id (object-id (fast-io:octets-from #(1 2 3 4 5 6 7 8 9 10 11 12))))
         (bson :_id (object-id #(1 2 3 4 5 6 7 8 9 10 11 12)))))

(assert (bson=
         (bson :_id (object-id (fast-io:octets-from #(1 2 3 4 5 6 7 8 9 10 11 12))))
         (bson :_id (object-id "0102030405060708090a0b0c"))))

(let ((bson (bson)))
  (setf (value bson :a) 1)
  (setf (value bson :a) 2)
  (assert (bson= (bson :a 2) bson)))

(let ((x (bson :a 1 :b 2))
      (y (bson :b 22 :c 33)))
  (assert (bson= (bson :a 1 :b 22 :c 33) (merge-bson x y))))
