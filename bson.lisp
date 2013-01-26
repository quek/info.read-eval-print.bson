;;;; info.read-eval-print.bson.lisp

(in-package #:info.read-eval-print.bson)

(defgeneric value (bson key))
(defgeneric (setf value) (value bson key))
(defgeneric bson= (x y))

(defclass bson ()
  ((head :initform nil)
   (tail :initform nil)))

(defmethod value ((bson bson) (key string))
  (with-slots (head) bson
    (cdr (assoc key head :test #'equal))))

(defmethod value ((bson bson) (key symbol))
  (with-slots (head) bson
    (cdr (assoc key head :test #'string-equal))))

(defmethod (setf value) (value (bson bson) (key string))
  (with-slots (head tail) bson
    (cond ((null head)
           (setf head (list (cons key value)))
           (setf tail head))
          (t
           (setf tail (setf (cdr tail) (list (cons key value))))))))

(defmethod (setf value) (value (bson bson) (key symbol))
  (setf (value bson (string-downcase key)) value))

(defmethod bson= ((x bson) (y bson))
  (labels ((f (a b)
             (if (null a)
                 (null b)
                 (and (equal (caar a) (caar b))
                      (equal (cdar a) (cdar b))
                      (f (cdr a) (cdr b))))))
    (f (slot-value x 'head) (slot-value y 'head))))

(defun bson (&rest args)
  (let ((bson (make-instance 'bson)))
    (loop for (key value) on args by #'cddr
          do (setf (value bson key) value))
    bson))

(defconstant +min-key+ '+min-key)
(defconstant +max-key+ '+max-key)
(defconstant +undefined+ '+undefined+)

(defclass binary-not-generic ()
  ((data :initarg :data :accessor data)))

(defclass binary-function (binary-not-generic) ())

(defclass binary-old (binary-not-generic) ())

(defclass binary-uuid-old (binary-not-generic) ())

(defclass binary-uuid (binary-not-generic) ())

(defclass binary-md5 (binary-not-generic) ())

(defclass binary-user-defined (binary-not-generic) ())

(defclass object-id ()
  ((data :initarg :data)))

(defclass db-pointer ()
  ((data :initarg :data :accessor data)))

(defclass javascript-code ()
  ((code :initarg :code :accessor code)))

(defclass code-with-scope (javascript-code)
  ((scope :initarg :scope :accessor scope)))

(defclass timestamp ()
  ((data :initarg :value :accessor date))
  (:documentation "Timestamp - Special internal type used by MongoDB
  replication and sharding. First 4 bytes are an increment, second 4
  are a timestamp. Setting the timestamp to 0 has special
  semantics."))


(defconstant +type-end-of-bson+ #x00 "End of BSON")
(defconstant +type-double+      #x01 "Floating point")
(defconstant +type-string+      #x02 "UTF-8 string")
(defconstant +type-document+    #x03 "Embedded document")
(defconstant +type-array+       #x04 "Array")
(defconstant +type-binary+      #x05 "Binary data")
(defconstant +type-undefined+   #x06 "Undefined — Deprecated")
(defconstant +type-object-id+   #x07 "ObjectId(byte*12)")
(defconstant +type-false+       #x08 "Boolean false (#x00)")
(defconstant +type-true+        #x08 "Boolean true (#x01)")
(defconstant +type-datetime+    #x09 "UTC datetime (int64)")
(defconstant +type-null+        #x0A "Null value")
(defconstant +type-regex+       #x0B "Regular expression (cstring)")
(defconstant +type-db-pointer+  #x0C "DBPointer — Deprecated (byte*12)")
(defconstant +type-java-script+ #x0D "JavaScript code (string)")
(defconstant +type-symbol+      #x0E "Symbol — Deprecated (string)")
(defconstant +type-code-w-s+    #x0F "JavaScript code w/ scope")
(defconstant +type-int32+       #x10 "32-bit Integer")
(defconstant +type-timestamp+   #x11 "Timestamp (int64)")
(defconstant +type-int64+       #x12 "64-bit integer")
(defconstant +type-min-key+     #xFF "Min key")
(defconstant +type-max-key+     #x7F "Max key")

(defun decode (binary)
  (declare (fast-io::octet-vector binary))
  (let ((bson (make-instance 'bson)))
    (fast-io:with-fast-input (in binary)
      (%decode in bson))))

(defun %decode (in bson)
  (let ((total-size (fast-io:read32-le in)))
    (declare (ignorable total-size))
    (loop for type = (fast-io:fast-read-byte in)
          until (= type +type-end-of-bson+)
          do (setf (value bson (decode-e-name in)) (decode-element type in)))
    bson))

(defun parse-cstring (in)
  (babel:octets-to-string
   (fast-io:with-fast-output (buffer)
     (loop for byte = (fast-io:fast-read-byte in)
           until (zerop byte)
           do (fast-io:fast-write-byte byte buffer)))
   :encoding :utf-8))

(defun parse-string (in)
  (let* ((size (1- (fast-io:read32-le in))) ;1- is null termination.
         (buffer (fast-io:make-octet-vector size)))
    (fast-io:fast-read-sequence buffer in)
    (fast-io:fast-read-byte in)   ;read #x00
    (babel:octets-to-string buffer :encoding :utf-8)))

(defun decode-e-name (in)
  (parse-cstring in))

(defgeneric decode-element (type in))

(defmethod decode-element ((type (eql +type-double+)) in)
  (ieee-floats:decode-float64 (fast-io:read64-le in)))

(defmethod decode-element ((type (eql +type-string+)) in)
  (parse-string in))

(defmethod decode-element ((type (eql +type-document+)) in)
  (%decode in (make-instance 'bson)))

(defmethod decode-element ((type (eql +type-array+)) in)
  (let ((total-size (fast-io:read32-le in)))
    (declare (ignorable total-size))
    (loop for type = (fast-io:fast-read-byte in)
          until (= type +type-end-of-bson+)
          collect (loop for x = (fast-io:fast-read-byte in)
                        until (zerop x)
                        finally (return (decode-element type in))))))

(defmethod decode-element ((type (eql +type-binary+)) in)
  (let* ((size (fast-io:read32-le in))
         (sub-type (fast-io:fast-read-byte in))
         (buffer (if (= #x02 sub-type)
                     (fast-io:make-octet-vector (setf size (fast-io:read32-le in)))
                     (fast-io:make-octet-vector size))))
    (fast-io:fast-read-sequence buffer in)
    (ecase sub-type
      (#x00                             ;Binary / Generic
       buffer)
      (#x01                             ;Function
       (make-instance 'binary-function :data buffer))
      (#x02                             ;Binary (Old)
       (make-instance 'binary-old :data buffer))
      (#x03                             ;UUID (Old)
       (make-instance 'binary-uuid-old :data buffer))
      (#x04                             ;UUID
       (make-instance 'binary-uuid :data buffer))
      (#x05                             ;MD5
       (make-instance 'binary-md5 :data buffer))
      (#x08                             ;User defined
       (make-instance 'binary-user-defined :data buffer)))))

(defmethod decode-element ((type (eql +type-undefined+)) in)
  +undefined+)

(defmethod decode-element ((type (eql +type-object-id+)) in)
  (let ((buffer (fast-io:make-octet-vector 12)))
    (fast-io:fast-read-sequence buffer in)
    (make-instance 'object-id :data buffer)))

(defmethod decode-element ((type (eql +type-false+)) in)
  (let ((value (fast-io:fast-read-byte in)))
    (assert (= #x00 value))
    nil))

(defmethod decode-element ((type (eql +type-true+)) in)
  (let ((value (fast-io:fast-read-byte in)))
    (assert (= #x01 value))
    t))

(defmethod decode-element ((type (eql +type-datetime+)) in)
  (let ((x (fast-io:read64-le in)))
    ;; x is UTC milliseconds since the Unix epoch.
    (local-time:unix-to-timestamp (truncate x 1000) :nsec (* (mod x 1000) 1000))))

(defmethod decode-element ((type (eql +type-null+)) in)
  nil)

(defmethod decode-element ((type (eql +type-regex+)) in)
  (let ((regex (parse-cstring in))
        (options (parse-cstring in)))
    (ppcre:create-scanner regex
                          :multi-line-mode (find #\m options)
                          :case-insensitive-mode (find #\i options))))

(defmethod decode-element ((type (eql +type-db-pointer+)) in)
  (let ((buffer (fast-io:make-octet-vector 12)))
    (fast-io:fast-read-sequence buffer in)
    (make-instance 'db-pointer :data buffer)))

(defmethod decode-element ((type (eql +type-java-script+)) in)
  (make-instance 'javascript-code :code (parse-string in)))

(defmethod decode-element ((type (eql +type-symbol+)) in)
  (intern (parse-string in)))

(defmethod decode-element ((type (eql +type-code-w-s+)) in)
  (let ((length (fast-io:read32-le in))
        (code (parse-string in))
        (scope (%decode in (make-instance 'bson))))
    (declare (ignore length))
    (make-instance 'code-with-scope :code code :scope scope)))

(defmethod decode-element ((type (eql +type-int32+)) in)
  (fast-io:read32-le in))

(defmethod decode-element ((type (eql +type-timestamp+)) in)
  (make-instance 'timestamp :data (fast-io:read64-le in)))

(defmethod decode-element ((type (eql +type-int64+)) in)
  (fast-io:read64-le in))

(defmethod decode-element ((type (eql +type-min-key+)) in)
  +min-key+)

(defmethod decode-element ((type (eql +type-max-key+)) in)
  +max-key+)
