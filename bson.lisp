;;;; info.read-eval-print.bson.lisp

(in-package #:info.read-eval-print.bson)

(defgeneric value (bson key))
(defgeneric (setf value) (value bson key))
(defgeneric bson= (x y))
(defgeneric encode (bson))

(defconstant +bson-true+ '+bson-true+)
(defconstant +bson-false+ '+bson-false+)
(defconstant +bson-undefined+ '+bson-undefined+)
(defconstant +bson-null+ '+bson-null+)
(defconstant +bson-empty-array+ '+bson-empty-array+)
(defconstant +bson-min-key+ '+bson-min-key+)
(defconstant +bson-max-key+ '+bson-max-key+)

(defclass bson ()
  ((head :initform nil)
   (tail :initform nil)))

(defmethod print-object ((bson bson) stream)
  (with-slots (head) bson
    (format stream "{~{~a~^, ~}}"
            (loop for (key . %value) in head
                  for value = (print-object-value %value)
                  collect (format nil "~s: ~a" key value)))))

(defun print-object-value (value)
  (case value
    (+bson-true+ "true")
    (+bson-false+ "false")
    (+bson-undefined+ "undefined")
    (+bson-null+ "null")
    (+bson-empty-array+ "[]")
    (+bson-min-key+ "+min-key+")
    (+bson-max-key+ "+max-key+")
    (t
     (if (consp value)
         (format nil "[~{~a~^, ~}]" (mapcar #'print-object-value value))
         (format nil "~s" value)))))

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
  (equalp (encode x) (encode y)))

(defmethod encode ((bson bson))
  (with-slots (head) bson
    (let* ((data (fast-io:with-fast-output (out)
                   (fast-io:write32-le 0 out) ;dummy document length.
                   (loop for (key . value) in head
                         for e-name = (babel:string-to-octets key :encoding :utf-8)
                         do (encode-element e-name value out))
                   (fast-io:fast-write-byte 0 out)))
           (length (length data)))
      (loop for i from 0 to 3
            do (setf (aref data i) (ldb (byte 8 (* i 8)) length)))
      data)))


(defun bson (&rest args)
  (let ((bson (make-instance 'bson)))
    (labels ((f (args)
               (if (endp args)
                   bson
                   (let ((car (car args)))
                     (if (consp car)
                         (progn
                           (setf (value bson (car car)) (cdr car))
                           (f (cdr args)))
                         (progn
                           (setf (value bson car) (cadr args))
                           (f (cddr args))))))))
      (f args))))

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

(defmethod print-object ((object-id object-id) stream)
  (format stream "ObjectId(\"~{~02,'0x~}\")"
          (map 'list #'identity (slot-value object-id 'data))))

(defclass regex ()
  ((regex :initarg :regex)
   (options :initarg :options :initform "")))

(defmethod print-object ((regex regex) stream)
  (with-slots (regex options) regex
    (format stream "/~a/~a" regex options)))

(defun regex (regex &optional (options ""))
  (make-instance 'regex :regex regex :options options))

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
(defconstant +type-boolean+     #x08 "Boolean true (#x01), false (#x00)")
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

(defgeneric decode (octet-vector-or-stream))

(defmethod decode ((vector vector))
  (fast-io:with-fast-input (in vector)
    (decode in)))

(defmethod decode ((stream stream))
  (fast-io:with-fast-input (in nil stream)
    (decode in)))

(defmethod decode ((in fast-io::input-buffer))
  (%decode in (make-instance 'bson)))

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
(defgeneric encode-element (e-name value out))

(defmacro def-encode ((type class value out) &body body)
  (let ((e-name (gensym "e-name")))
    `(defmethod encode-element (,e-name (,value ,class) ,out)
       (fast-io:fast-write-byte ,type ,out)
       (fast-io:fast-write-sequence ,e-name ,out)
       (fast-io:fast-write-byte 0 ,out)
       ,@body)))

(defmethod decode-element ((type (eql +type-double+)) in)
  (ieee-floats:decode-float64 (fast-io:read64-le in)))

(def-encode (+type-double+ float value out)
  (fast-io:write64-le (ieee-floats:encode-float64 value) out))

(defmethod decode-element ((type (eql +type-string+)) in)
  (parse-string in))

(defun encode-string (string out)
  (let ((x (babel:string-to-octets string :encoding :utf-8)))
    (fast-io:write32-le (1+ (length x)) out) ;1+ is null termination.
    (fast-io:fast-write-sequence x out)
    (fast-io:fast-write-byte 0 out)))

(def-encode (+type-string+ string value out)
  (encode-string value out))

(defmethod decode-element ((type (eql +type-document+)) in)
  (%decode in (make-instance 'bson)))

(def-encode (+type-document+ bson value out)
  (fast-io:fast-write-sequence (encode value) out))

(defmethod decode-element ((type (eql +type-array+)) in)
  (let ((total-size (fast-io:read32-le in)))
    (declare (ignorable total-size))
    (loop for type = (fast-io:fast-read-byte in)
          until (= type +type-end-of-bson+)
          collect (loop for x = (fast-io:fast-read-byte in)
                        until (zerop x)
                        finally (return (decode-element type in))))))

(def-encode (+type-array+ list value out)
  (let ((bson (make-instance 'bson)))
    (loop for i from 0
          for x in value
          do (setf (value bson (princ-to-string i)) x))
    (fast-io:fast-write-sequence (encode bson) out)))

(def-encode (+type-array+ (eql +bson-empty-array+) value out)
  (fast-io:fast-write-sequence (encode (bson)) out))

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

(defgeneric binary-sub-type (x)
  (:method ((x vector))
    #x00))

(def-encode (+type-binary+ vector value out)
  (fast-io:write32-le (length value) out)
  (fast-io:fast-write-byte (binary-sub-type value) out)
  (fast-io:fast-write-sequence value out))

(def-encode (+type-binary+ binary-not-generic value out)
  (with-slots (data) value
    (fast-io:write32-le (length data) out)
    (fast-io:fast-write-byte (binary-sub-type value) out)
    (fast-io:fast-write-sequence data out)))

(def-encode (+type-binary+ binary-old value out)
  (with-slots (data) value
    (fast-io:write32-le (+ (length data) 4) out)
    (fast-io:fast-write-byte (binary-sub-type value) out)
    (fast-io:write32-le (length data) out)
    (fast-io:fast-write-sequence data out)))

(defmethod decode-element ((type (eql +type-undefined+)) in)
  +bson-undefined+)

(def-encode (+type-undefined+ (eql +bson-undefined+) value out))

(defmethod decode-element ((type (eql +type-object-id+)) in)
  (let ((buffer (fast-io:make-octet-vector 12)))
    (fast-io:fast-read-sequence buffer in)
    (make-instance 'object-id :data buffer)))

(def-encode (+type-object-id+ object-id value out)
  (with-slots (data) value
    (fast-io:fast-write-sequence data out)))

(defmethod decode-element ((type (eql +type-boolean+)) in)
  (let ((value (fast-io:fast-read-byte in)))
    (= #x01 value)))

(def-encode (+type-boolean+ (eql +bson-false+) value out)
  (fast-io:fast-write-byte #x00 out))
(def-encode (+type-boolean+ (eql t) value out)
  (fast-io:fast-write-byte #x01 out))
(def-encode (+type-boolean+ (eql +bson-true+) value out)
  (fast-io:fast-write-byte #x01 out))

(defmethod decode-element ((type (eql +type-datetime+)) in)
  (let ((x (fast-io:read64-le in)))
    ;; x is UTC milliseconds since the Unix epoch.
    (local-time:unix-to-timestamp (truncate x 1000) :nsec (* (mod x 1000) 1000))))

(def-encode (+type-datetime+ local-time:timestamp value out)
  (let ((unix (local-time:timestamp-to-unix value))
        (nsec (local-time:nsec-of value)))
    (fast-io:write64-le (+ (* unix 1000) nsec))))

(defmethod decode-element ((type (eql +type-null+)) in)
  nil)

(def-encode (+type-null+ (eql +bson-null+) value out))
(def-encode (+type-null+ null value out))

(defmethod to-ppcre-regex ((regex regex))
  (with-slots (regex options) regex
    (ppcre:create-scanner regex
                          :multi-line-mode (find #\m options)
                          :case-insensitive-mode (find #\i options))))

(defmethod decode-element ((type (eql +type-regex+)) in)
  (let ((regex (parse-cstring in))
        (options (parse-cstring in)))
    (make-instance 'regex :regex regex :options options)))

(def-encode (+type-regex+ regex value out)
  (with-slots (regex options) value
    (encode-string regex out)
    (encode-string options out)))

(defmethod decode-element ((type (eql +type-db-pointer+)) in)
  (let ((buffer (fast-io:make-octet-vector 12)))
    (fast-io:fast-read-sequence buffer in)
    (make-instance 'db-pointer :data buffer)))

(def-encode (+type-db-pointer+ db-pointer value out)
  (with-slots (data) value
    (fast-io:fast-write-sequence data out)))

(defmethod decode-element ((type (eql +type-java-script+)) in)
  (make-instance 'javascript-code :code (parse-string in)))

(def-encode (+type-java-script+ javascript-code value out)
  (with-slots (code) value
   (encode-string code out)))

(defmethod decode-element ((type (eql +type-symbol+)) in)
  (intern (parse-string in)))

(def-encode (+type-symbol+ symbol value out)
  (encode-string (symbol-name value) out))

(defmethod decode-element ((type (eql +type-code-w-s+)) in)
  (let ((length (fast-io:read32-le in))
        (code (parse-string in))
        (scope (%decode in (make-instance 'bson))))
    (declare (ignore length))
    (make-instance 'code-with-scope :code code :scope scope)))

(def-encode (+type-code-w-s+ code-with-scope value out)
  (with-slots (code scope) value
    (let* ((code (babel:string-to-octets code :encoding :utf-8))
           (scope (encode scope))
           (length (+ (length code) 1 (length scope))))
      (fast-io:write32-le length out)
      (fast-io:fast-write-sequence code out)
      (fast-io:fast-write-byte 0 out)
      (fast-io:fast-write-sequence scope out))))

(defmethod decode-element ((type (eql +type-int32+)) in)
  (fast-io:read32-le in))

(defmethod encode-element (e-name (value integer) out)
  (cond ((< -2147483648 value 2147483647)
         (fast-io:fast-write-byte +type-int32+ out)
         (fast-io:fast-write-sequence e-name out)
         (fast-io:fast-write-byte 0 out)
         (fast-io:write32-le value out))
        ((<  -9223372036854775808 value 9223372036854775807)
         (fast-io:fast-write-byte +type-int64+ out)
         (fast-io:fast-write-sequence e-name out)
         (fast-io:fast-write-byte 0 out)
         (fast-io:write64-le value out))
        (t
         (error "An integer value is too small or too large: ~d." value))))

(defmethod decode-element ((type (eql +type-timestamp+)) in)
  (make-instance 'timestamp :data (fast-io:read64-le in)))

(def-encode (+type-timestamp+ timestamp value out)
  (with-slots (data) value
    (fast-io:write64-le value out)))

(defmethod decode-element ((type (eql +type-int64+)) in)
  (fast-io:read64-le in))

(defmethod decode-element ((type (eql +type-min-key+)) in)
  +bson-min-key+)

(def-encode (+type-min-key+ (eql +bson-min-key+) value out))

(defmethod decode-element ((type (eql +type-max-key+)) in)
  +bson-max-key+)

(def-encode (+type-max-key+ (eql +bson-max-key+) value out))
